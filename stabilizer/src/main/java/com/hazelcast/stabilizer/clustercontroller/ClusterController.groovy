package com.hazelcast.stabilizer.clustercontroller

import com.google.common.base.Predicate
import com.hazelcast.logging.ILogger
import com.hazelcast.logging.Logger
import com.hazelcast.stabilizer.Utils
import com.hazelcast.stabilizer.agent.AgentRemoteService
import com.hazelcast.stabilizer.agent.workerjvm.WorkerJvmManager
import org.jclouds.ContextBuilder
import org.jclouds.compute.ComputeService
import org.jclouds.compute.ComputeServiceContext
import org.jclouds.compute.domain.ExecResponse
import org.jclouds.compute.domain.NodeMetadata
import org.jclouds.compute.domain.Template
import org.jclouds.compute.domain.TemplateBuilderSpec
import org.jclouds.compute.options.RunScriptOptions
import org.jclouds.domain.LoginCredentials
import org.jclouds.logging.log4j.config.Log4JLoggingModule
import org.jclouds.scriptbuilder.ExitInsteadOfReturn
import org.jclouds.scriptbuilder.domain.Statement
import org.jclouds.scriptbuilder.domain.StatementList
import org.jclouds.scriptbuilder.domain.Statements
import org.jclouds.scriptbuilder.domain.chef.RunList
import org.jclouds.scriptbuilder.statements.chef.ChefSolo
import org.jclouds.scriptbuilder.statements.chef.InstallChefUsingOmnibus
import org.jclouds.scriptbuilder.statements.git.CloneGitRepo
import org.jclouds.scriptbuilder.statements.git.InstallGit
import org.jclouds.scriptbuilder.statements.login.AdminAccess
import org.jclouds.sshj.config.SshjSshClientModule

import static com.hazelcast.stabilizer.Utils.*
import static java.lang.String.format
import static java.util.Arrays.asList
import static org.jclouds.compute.options.RunScriptOptions.Builder.overrideAuthenticateSudo

//https://jclouds.apache.org/start/compute/ good read
public class ClusterController {
    private final static ILogger log = Logger.getLogger(ClusterController.class.getName());

    def config
    def STABILIZER_HOME = Utils.getStablizerHome().getAbsolutePath()
    def CONF_DIR = new File(STABILIZER_HOME, "conf");
    def agentsFile = new File("agents.txt")

    List<String> privateIps = []

    ClusterController() {
        log.info("Hazelcast Stabilizer ClusterController");
        log.info(format("Version: %s", getVersion()));
        log.info(format("STABILIZER_HOME: %s", STABILIZER_HOME));

        Properties props = new Properties()

        new File("stabilizer.properties").withInputStream {
            stream -> props.load(stream)
        }
        config = new ConfigSlurper().parse(props)

        if (!agentsFile.exists()) {
            agentsFile.createNewFile()
        }
        agentsFile.text.eachLine { String line -> privateIps << line }
    }

    void installAgent(String ip) {
        echoImportant "Installing Agent on ${ip}"

        echo "Copying stabilizer files"
        //first we remove the old lib files to prevent different versions of the same jar to bite us.
        sshQuiet ip, "rm -fr hazelcast-stabilizer-${getVersion()}/lib"
        //then we copy the stabilizer directory
        scpToRemote ip, STABILIZER_HOME, ""


        echoImportant("Successfully installed Agent on ${ip}");
    }


    void startAgents() {
        echoImportant("Starting ${privateIps.size()} Agents");

        for (String ip : privateIps) {
            echo "Killing Agent $ip"
            ssh ip, "killall -9 java || true"
        }

        for (String ip : privateIps) {
            echo "Starting Agent $ip"
            ssh ip, "nohup hazelcast-stabilizer-${getVersion()}/bin/agent > agent.out 2> agent.err < /dev/null &"
        }

        echoImportant("Successfully started ${privateIps.size()} Agents");
    }

    void killAgents() {
        echoImportant("Killing ${privateIps.size()} Agents");

        for (String ip : privateIps) {
            echo "Killing Agent $ip"
            ssh ip, "killall -9 java || true"
        }

        echoImportant("Successfully killed ${privateIps.size()} Agents");
    }

    void installAgents() {
        privateIps.each { String ip -> installAgent(ip) }
    }

    int calcSize(String sizeType) {
        try {
            return Integer.parseInt(sizeType)
        } catch (NumberFormatException e) {
            println "Unknown size: $sizeType"
            System.exit 1
        }
    }

    void scale(String sizeType) {
        int delta = calcSize(sizeType) - privateIps.size();
        if (delta == 0) {
            echo("Ignoring spawn machines, desired number of machines already exists");
        } else if (delta < 0) {
            terminate(-delta);
        } else {
            scaleUp(delta);
        }
    }

    private int[] inboundPorts() {
        List<Integer> ports = new ArrayList<Integer>();
        ports.add(22);
        //todo:the following 2 ports should not be needed
        ports.add(443);
        ports.add(80);
        ports.add(AgentRemoteService.PORT);
        ports.add(WorkerJvmManager.PORT);
        for (int k = 5701; k < 5901; k++) {
            ports.add(k);
        }

        int[] result = new int[ports.size()];
        for (int k = 0; k < result.length; k++) {
            result[k] = ports[k];
        }
        return result;
    }

    private void scaleUp(int delta) {
        echoImportant("Starting ${delta} ${config.CLOUD_PROVIDER} machines");
        echo(config.MACHINE_SPEC);

        ComputeService compute = getComputeService()

        echo("Created compute")

        Template template = compute.templateBuilder()
                .from(TemplateBuilderSpec.parse(config.MACHINE_SPEC))
                .build();

        echo("Created template")

        template.getOptions()
                .inboundPorts(inboundPorts())
//                .authorizePublicKey(fileAsText(config.PUBLIC_KEY))
//                .blockUntilRunning(true)
               .securityGroups(config.SECURITY_GROUP)

        echo("Creating nodes")

        Set<NodeMetadata> nodes = compute.createNodesInGroup("stabilizer-agent", delta, template)
        echo("Created machines, waiting for startup (can take a few minutes)")

        for (NodeMetadata m : nodes) {
            String ip = m.privateAddresses.iterator().next()
            echo("\t" + ip + " LAUNCHED");
            appendText(ip + "\n", agentsFile)
        }

        if ("provisioned".equals(config.JDK_FLAVOR)) {
            log.info("Skipping Java installation");
        } else {
            log.info("Installing Java: ${config.JDK_FLAVOR} ${config.JDK_VERSION}")
            for (NodeMetadata m : nodes) {
                String ip = m.privateAddresses.iterator().next()
                installChef(m, compute);
                installJava(m, compute);
                echo("\t" + ip + " JAVA INSTALLED");
            }
        }

        LoginCredentials login = LoginCredentials.builder()
                .privateKey(fileAsText(config.IDENTITY_FILE))
                .user(config.USER)
                .noPassword()
                .build();

        RunScriptOptions runScriptOptions = RunScriptOptions.Builder
                .overrideLoginCredentials(login)
                .wrapInInitScript(false)

        //waiting for nodes to start
        for (NodeMetadata m : nodes) {
            String node = m.getId();

            String ip = m.privateAddresses.iterator().next()

            ExecResponse response = compute.runScriptOnNode(node, Statements.exec("ls -al"), runScriptOptions);
            if (response.exitStatus != 0) {
                exitWithError(format("Could not successfully ssh to %s", ip))
            }
            echo("\t" + ip + " STARTED");
            privateIps.add(ip)
        }

        echoImportant("Successfully started ${delta} ${config.CLOUD_PROVIDER} machines ");

        installAgents()
        startAgents()
    }

    private ComputeService getComputeService() {
        return ContextBuilder.newBuilder(config.CLOUD_PROVIDER)
                .credentials(config.CLOUD_IDENTITY, config.CLOUD_CREDENTIAL)
                .modules(asList(new Log4JLoggingModule(), new SshjSshClientModule()))
                .buildView(ComputeServiceContext.class)
                .getComputeService();
    }

    private void installChef(NodeMetadata node, ComputeService compute) {
        echo("Bootstrapping...");

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

//        LoginCredentials login = LoginCredentials.builder()
//                .privateKey(fileAsText(config.IDENTITY_FILE))
//                .user(config.USER)
//                .noPassword()
//                .build();
//
//        RunScriptOptions runScriptOptions = RunScriptOptions.Builder
//                .overrideLoginCredentials(login)
//                .overrideAuthenticateSudo(true)
//                .wrapInInitScript(false);

        ExecResponse response = compute.runScriptOnNode(node.getId(), statement, overrideAuthenticateSudo(true));

        echo("------------------------------------------------------------------------------");
        echo("Exit code install java: " + response.getExitStatus());
        echo("------------------------------------------------------------------------------");
//
        if (response.exitStatus != 0) {
            log.severe("Failed to install chef on machine: " + node.privateAddresses.iterator().next());
            log.severe(response.output);
            log.severe(response.error);
            System.exit(1)
        }
    }

    //https://gist.github.com/nacx/7317938
    //https://github.com/socrata-cookbooks/java/blob/master/metadata.rb
    private void installJava(NodeMetadata node, ComputeService compute) {
        echo("Installing Java...");

        Statement cloneJavaCookbook = CloneGitRepo.builder()
                .repository("https://github.com/socrata-cookbooks/java.git")
                .directory("/var/chef/cookbooks/java")
                .build();

        File file = new File(CONF_DIR, "java_chef.json");
        String javaAttributes = Utils.fileAsText(file);

        String JDK_FLAVOR = config.JDK_FLAVOR;
        String JDK_VERSION = config.JDK_VERSION;
        String IBM_JDK_6_URL = config.IBM_JDK_6_URL;
        String IBM_JDK_7_URL = config.IBM_JDK_7_URL;
        String IBM_JDK_URL = "6".equals(JDK_VERSION) ? IBM_JDK_6_URL : IBM_JDK_7_URL;

        javaAttributes = javaAttributes
                .replace('$JDK_FLAVOR', JDK_FLAVOR)
                .replace('$JDK_VERSION', JDK_VERSION)
                .replace('$IBM_JDK_URL', IBM_JDK_URL);

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

//        LoginCredentials login = LoginCredentials.builder()
//                .privateKey(fileAsText(config.IDENTITY_FILE))
//                .user(config.USER)
//                .noPassword()
//                .build();
//
//        RunScriptOptions runScriptOptions = overrideAuthenticateSudo(true);

        ExecResponse response = compute.runScriptOnNode(node.getId(), statement, overrideAuthenticateSudo(true));

        echo("------------------------------------------------------------------------------");
        echo("Exit code install java: " + response.getExitStatus());
        echo("------------------------------------------------------------------------------");

        log.info(response.output);
        log.info(response.error);

        if (response.exitStatus != 0) {
            log.severe("Failed to install Java on machine: " + node.privateAddresses.iterator().next());
            log.severe(response.output);
            log.severe(response.error);
            System.exit(1)
        }

    }

    def downloadArtifacts() {
        echoImportant("Download artifacts of ${privateIps.size()} machines");

        bash("mkdir -p workers");

        for (String ip : privateIps) {
            echo("Downoading from $ip");

            bash("""rsync -av -e "ssh ${config.SSH_OPTIONS}" \
                ${config.USER}@$ip:hazelcast-stabilizer-${getVersion()}/workers/ \
                workers""");
        }

        echoImportant("Finished Downloading Artifacts of ${privateIps.size()} machines");
    }

    void terminate(count = Integer.MAX_VALUE) {
        if (count > privateIps.size()) {
            count = privateIps.size();
        }

        echoImportant(format("Terminating %s %s machines", count, config.CLOUD_PROVIDER));

        final List<String> terminateList = privateIps.subList(0, count);

        ComputeService computeService = getComputeService();
        computeService.destroyNodesMatching(
                new Predicate<NodeMetadata>() {
                    @Override
                    boolean apply(NodeMetadata nodeMetadata) {
                        for (String ip : nodeMetadata.privateAddresses) {
                            if (terminateList.remove(ip)) {
                                echo(format("\t%s Terminating", ip))
                                privateIps.remove(ip)
                                return true;
                            }
                        }
                        return false;
                    }
                }
        )

        log.info("Updating " + agentsFile.getAbsolutePath());
        agentsFile.write("")
        for (String ip : privateIps) {
            agentsFile.text += "$ip\n"
        }

        echoImportant("Finished terminating $count ${config.CLOUD_PROVIDER} machines");
    }

    void bash(String command) {
        def sout = new StringBuffer(), serr = new StringBuffer()

        // create a process for the shell
        ProcessBuilder pb = new ProcessBuilder("bash", "-c", command);
        Process shell = pb.start();
        shell.consumeProcessOutput(sout, serr)

        // wait for the shell to finish and get the return code
        int shellExitStatus = shell.waitFor();
        if (shellExitStatus != 0) {
            echo("Failed to execute [$command]");
            println "out> $sout err> $serr"
            System.exit 1
        }
    }

    void scpToRemote(String ip, String src, String target) {
        String command = "scp -r ${config.SSH_OPTIONS} $src ${config.USER}@$ip:$target";
        bash(command);
    }

    void ssh(String ip, String command) {
        String sshCommand = "ssh ${config.SSH_OPTIONS} -q ${config.USER}@$ip \"$command\"";
        bash(sshCommand);
    }

    void sshQuiet(String ip, String command) {
        String sshCommand = "ssh ${config.SSH_OPTIONS} -q ${config.USER}@$ip \"$command\" || true";
        bash(sshCommand);
    }

    void echo(Object s) {
        log.info(s == null ? "null" : s.toString());
    }

    void echoImportant(Object s) {
        echo("==============================================================");
        echo(s);
        echo("==============================================================");
    }

    public static void main(String[] args) {
        def cli = new CliBuilder(
                usage: 'cluster [options]',
                header: '\nAvailable options (use -h for help):\n',
                stopAtNonOption: false)
        cli.with {
            h(longOpt: 'help', 'print this message')
            r(longOpt: 'restart', 'Restarts all agents')
            d(longOpt: 'download', 'Downloads the logs')
            s(longOpt: 'scale', args: 1, 'Scales the cluster')
            t(longOpt: 'terminate', 'Terminate all members in the cluster')
            k(longOpt: 'kill', 'Kills all agents')
        }

        OptionAccessor opt = cli.parse(args)

        if (!opt) {
            println "Failure parsing options"
            System.exit 1
            return
        }

        if (opt.h) {
            cli.usage()
            System.exit 0
        }

        if (opt.r) {
            def cluster = new ClusterController()
            cluster.installAgents()
            cluster.startAgents()
            System.exit 0
        }

        if (opt.k) {
            def cluster = new ClusterController()
            cluster.killAgents()
            System.exit 0
        }


        if (opt.d) {
            def cluster = new ClusterController()
            cluster.downloadArtifacts()
            System.exit 0
        }

        if (opt.t) {
            def cluster = new ClusterController()
            cluster.terminate()
            System.exit 0
        }

        if (opt.s) {
            def cluster = new ClusterController()

            String sizeType = opt.s
            cluster.scale(sizeType)
            System.exit 0
        }
    }
}
