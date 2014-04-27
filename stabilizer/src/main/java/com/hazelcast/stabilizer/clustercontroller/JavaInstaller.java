package com.hazelcast.stabilizer.clustercontroller;

import com.google.common.collect.ImmutableSet;
import com.hazelcast.stabilizer.Utils;
import org.jclouds.ContextBuilder;
import org.jclouds.View;
import org.jclouds.aws.ec2.AWSEC2Api;
import org.jclouds.aws.ec2.domain.AWSRunningInstance;
import org.jclouds.compute.ComputeService;
import org.jclouds.compute.ComputeServiceContext;
import org.jclouds.compute.domain.ExecResponse;
import org.jclouds.compute.domain.NodeMetadata;
import org.jclouds.compute.domain.Template;
import org.jclouds.ec2.compute.domain.EC2HardwareBuilder;
import org.jclouds.logging.log4j.config.Log4JLoggingModule;
import org.jclouds.scriptbuilder.ExitInsteadOfReturn;
import org.jclouds.scriptbuilder.domain.Statement;
import org.jclouds.scriptbuilder.domain.StatementList;
import org.jclouds.scriptbuilder.domain.Statements;
import org.jclouds.scriptbuilder.domain.chef.RunList;
import org.jclouds.scriptbuilder.statements.chef.ChefSolo;
import org.jclouds.scriptbuilder.statements.chef.InstallChefUsingOmnibus;
import org.jclouds.scriptbuilder.statements.git.CloneGitRepo;
import org.jclouds.scriptbuilder.statements.git.InstallGit;
import org.jclouds.scriptbuilder.statements.login.AdminAccess;
import org.jclouds.sshj.config.SshjSshClientModule;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Arrays.asList;
import static org.jclouds.compute.options.RunScriptOptions.Builder.overrideAuthenticateSudo;
import static org.jclouds.compute.options.TemplateOptions.Builder.inboundPorts;

public class JavaInstaller {
    private final ComputeService compute;
    private final ComputeServiceContext context;
    private Properties properties = loadProperties();
    String imageId = "us-east-1/ami-018c9568"; // Ubuntu 12.04.3 LTS (64-bit)
    int[] openPorts = new int[]{22, 80, 443}; // Ports to open in the node

    public JavaInstaller() {
        // CONFIGURATION

        String identity = "foo";
        String credential = "bar";

        // END CONFIGURATION

        context =
                ContextBuilder.newBuilder("aws-ec2").credentials(identity, credential)
                        .modules(ImmutableSet.of(new Log4JLoggingModule(), new SshjSshClientModule()))
                        .buildView(ComputeServiceContext.class);
        compute = context.getComputeService();
    }

    public static void main(final String[] args) throws Exception {
        JavaInstaller installer = new JavaInstaller();
        installer.install();
    }

    public void install() throws Exception {
        if ("provisioned".equals(properties.getProperty("JDK_FLAVOR"))) {
            System.out.println("skipping java installation, nodes are configured with 'provisioned'");
            return;
        }

        try {
            System.out.println("Looking for the template...");

            Template template = compute.templateBuilder() //
                    .fromHardware(EC2HardwareBuilder.t1_micro().build()) // Deploy the smallest template
                    .imageId(imageId) //
                    .options(inboundPorts(openPorts)) //
                    .build();

            System.out.println("Provisioning node...");

            NodeMetadata node = getOnlyElement(compute.createNodesInGroup("chef-server", 1, template));
            String publicDns = findPublicDns(node, context);

            installAndConfigureChef(node);

            installAndConfigureJava(node);

            System.out.println("Done! Access your node at: " + publicDns);
        } finally {
            context.close();
        }
    }

    private Properties loadProperties() {
        try {
            File file = new File("/java/projects/Hazelcast/hazelcast-stabilizer/dist/src/main/dist/bin/start.properties");
            Properties properties = new Properties();
            FileInputStream inStream = new FileInputStream(file);
            try {
                properties.load(inStream);
            } finally {
                Utils.closeQuietly(inStream);
            }
            return properties;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void installAndConfigureChef(NodeMetadata node) {
        System.out.println("Bootstrapping...");

        Statement cloneCookbooks = CloneGitRepo.builder()
                .repository("https://github.com/opscode-cookbooks/chef-server.git")
                .directory("/var/chef/cookbooks/chef-server")
                .build();

        Statement statement = new StatementList(
                AdminAccess.standard(),
                new ExitInsteadOfReturn(new InstallGit()),
                cloneCookbooks,
                new InstallChefUsingOmnibus()//,
        );

        ExecResponse checkResponse = compute.runScriptOnNode(node.getId(), statement, overrideAuthenticateSudo(true));

        System.out.println("------------------------------------------------------------------------------");
        System.out.println("Exit code install chef: " + checkResponse.getExitStatus());
        System.out.println("------------------------------------------------------------------------------");

        if (checkResponse.getExitStatus() != 0) {
            System.out.println(checkResponse.getError());
            System.out.print(checkResponse.getOutput());
        }
    }

    //https://gist.github.com/nacx/7317938
    //https://github.com/socrata-cookbooks/java/blob/master/metadata.rb
    public void installAndConfigureJava(NodeMetadata node) {
        System.out.println("Installing Java...");

        Statement cloneJavaCookbook = CloneGitRepo.builder()
                .repository("https://github.com/socrata-cookbooks/java.git")
                .directory("/var/chef/cookbooks/java")
                .build();

        File file = new File(Utils.getStablizerHome() + Utils.FILE_SEPERATOR + "conf" + Utils.FILE_SEPERATOR + "java_chef.json");
        String javaAttributes = Utils.fileAsText(file);

        String JDK_FLAVOR = properties.getProperty("JDK_FLAVOR");
        String JDK_VERSION = properties.getProperty("JDK_VERSION");
        String IBM_JDK_6_URL = properties.getProperty("IBM_JDK_6_URL");
        String IBM_JDK_7_URL = properties.getProperty("IBM_JDK_7_URL");
        String IBM_JDK_URL = "6".equals(JDK_VERSION) ? IBM_JDK_6_URL : IBM_JDK_7_URL;

        javaAttributes = javaAttributes
                .replace("$JDK_FLAVOR", JDK_FLAVOR)
                .replace("$JDK_VERSION", JDK_VERSION)
                .replace("$IBM_JDK_URL", IBM_JDK_URL);

        System.out.println(javaAttributes);

        Statement installJava = ChefSolo.builder()
                .cookbookPath("/var/chef/cookbooks")
                .runlist(RunList.builder().recipes(asList("java::default")).build())
                .jsonAttributes(javaAttributes)
                .build();

        Statement statement = new StatementList(
                AdminAccess.standard(),
                cloneJavaCookbook,
                installJava,
                Statements.exec("java -version"));

        ExecResponse javaResponse = compute.runScriptOnNode(node.getId(), statement, overrideAuthenticateSudo(true));

        System.out.println("------------------------------------------------------------------------------");
        System.out.println("Exit code install java: " + javaResponse.getExitStatus());
        System.out.println("------------------------------------------------------------------------------");
        //if (javaResponse.getExitStatus() != 0) {
        System.out.println(javaResponse.getError());
        System.out.print(javaResponse.getOutput());
    }

    private String findPublicDns(final NodeMetadata node, final View view) {
        AWSEC2Api aws = view.unwrapApi(AWSEC2Api.class);
        AWSRunningInstance instance =
                getOnlyElement(getOnlyElement(aws.getInstanceApi().get()
                        .describeInstancesInRegion(null, node.getProviderId())));
        return instance.getDnsName();
    }
}