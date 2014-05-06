package com.hazelcast.stabilizer.provisioner;

import com.google.common.base.Predicate;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.Utils;
import com.hazelcast.stabilizer.agent.AgentRemoteService;
import com.hazelcast.stabilizer.agent.workerjvm.WorkerJvmManager;
import com.hazelcast.stabilizer.common.AgentAddress;
import com.hazelcast.stabilizer.common.AgentsFile;
import joptsimple.OptionException;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.jclouds.ContextBuilder;
import org.jclouds.compute.ComputeService;
import org.jclouds.compute.ComputeServiceContext;
import org.jclouds.compute.domain.NodeMetadata;
import org.jclouds.compute.domain.Template;
import org.jclouds.compute.domain.TemplateBuilderSpec;
import org.jclouds.logging.log4j.config.Log4JLoggingModule;
import org.jclouds.scriptbuilder.statements.login.AdminAccess;
import org.jclouds.sshj.config.SshjSshClientModule;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.hazelcast.stabilizer.Utils.appendText;
import static com.hazelcast.stabilizer.Utils.getVersion;
import static com.hazelcast.stabilizer.Utils.secondsToHuman;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static org.jclouds.compute.config.ComputeServiceProperties.POLL_INITIAL_PERIOD;
import static org.jclouds.compute.config.ComputeServiceProperties.POLL_MAX_PERIOD;

//https://jclouds.apache.org/start/compute/ good read
//https://github.com/jclouds/jclouds-examples/blob/master/compute-basics/src/main/java/org/jclouds/examples/compute/basics/MainApp.java
//https://github.com/jclouds/jclouds-examples/blob/master/minecraft-compute/src/main/java/org/jclouds/examples/minecraft/NodeManager.java
public class Provisioner {
    private final static ILogger log = Logger.getLogger(Provisioner.class.getName());

    private final Properties stabilizerProperties = loadStabilizerProperties();
    private final String VERSION = Utils.getVersion();
    private final String CLOUD_PROVIDER = getProperty("CLOUD_PROVIDER");

    private final String STABILIZER_HOME = Utils.getStablizerHome().getAbsolutePath();
    private final File CONF_DIR = new File(STABILIZER_HOME, "conf");
    private final File JDK_INSTALL_DIR = new File(STABILIZER_HOME, "jdk-install");
    private final File agentsFile = new File("agents.txt");
    //big number of threads, but they are used to offload ssh tasks. So there is no load on this machine..
    private final ExecutorService executor = Executors.newFixedThreadPool(10);

    private final List<AgentAddress> addresses = Collections.synchronizedList(new LinkedList<AgentAddress>());

    public Provisioner() throws Exception {
        log.info("Hazelcast Stabilizer Provisioner");
        log.info(format("Version: %s", getVersion()));
        log.info(format("STABILIZER_HOME: %s", STABILIZER_HOME));

        if (!agentsFile.exists()) {
            agentsFile.createNewFile();
        }

        addresses.addAll(AgentsFile.load(agentsFile));
    }

    private static Properties loadStabilizerProperties() {
        Properties properties = new Properties();
        File file = new File("stabilizer.properties");

        try {
            FileInputStream inputStream = new FileInputStream(file);
            try {
                properties.load(inputStream);
            } catch (IOException e) {
                Utils.closeQuietly(inputStream);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return properties;
    }

    void installAgent(String ip) {
        //first we remove the old lib files to prevent different versions of the same jar to bite us.
        sshQuiet(ip, format("rm -fr hazelcast-stabilizer-%s/lib", VERSION));

        //then we copy the stabilizer directory
        scpToRemote(ip, STABILIZER_HOME, "");

        String versionSpec = getProperty("HAZELCAST_VERSION_SPEC", "outofthebox");
        if (!versionSpec.equals("outofthebox")) {
            //remove the hazelcast jars, they will be copied from the 'hazelcastJarsDir'.
            ssh(ip, format("rm hazelcast-stabilizer-%s/lib/hazelcast-*.jar", VERSION));
            //copy the actual hazelcast jars that are going to be used by the worker.
            scpToRemote(ip, hazelcastJarsDir.getAbsolutePath() + "/*.jar", format("hazelcast-stabilizer-%s/lib", VERSION));
        }
    }

    public void startAgents() {
        echoImportant("Starting %s Agents", addresses.size());

        for (AgentAddress address : addresses) {
            echo("Killing Agent %s", address.publicAddress);
            ssh(address.publicAddress, "killall -9 java || true");
        }

        for (AgentAddress address : addresses) {
            echo("Starting Agent %s", address.publicAddress);
            ssh(address.publicAddress, format("nohup hazelcast-stabilizer-%s/bin/agent > agent.out 2> agent.err < /dev/null &", VERSION));
        }

        echoImportant("Successfully started %s Agents", addresses.size());
    }

    void startAgent(String ip) {
        ssh(ip, "killall -9 java || true");
        ssh(ip, format("nohup hazelcast-stabilizer-%s/bin/agent > agent.out 2> agent.err < /dev/null &", getVersion()));
    }

    void killAgents() {
        echoImportant("Killing %s Agents", addresses.size());

        for (AgentAddress address : addresses) {
            echo("Killing Agent, %s", address.publicAddress);
            ssh(address.publicAddress, "killall -9 java || true");
        }

        echoImportant("Successfully killed %s Agents", addresses.size());
    }

    public void restart() {
        prepareHazelcastJars();
        for (AgentAddress address : addresses) {
            installAgent(address.publicAddress);
        }
    }

    public void scale(int size) throws Exception {
        int delta = size - addresses.size();
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
            result[k] = ports.get(k);
        }
        return result;
    }

    private void scaleUp(int delta) throws Exception {
        echoImportant("Provisioning %s %s machines", delta, CLOUD_PROVIDER);
        echo(getProperty("MACHINE_SPEC"));

        long startTimeMs = System.currentTimeMillis();

        String jdkFlavor = getProperty("JDK_FLAVOR");
        if ("outofthebox".equals(jdkFlavor)) {
            log.info("Machines will use Java: Out of the Box.");
        } else {
            log.info(format("Machines will use Java: %s %s", jdkFlavor, getProperty("JDK_VERSION")));
        }

        prepareHazelcastJars();

        ComputeService compute = getComputeService();

        echo("Created compute");

        Template template = compute.templateBuilder()
                .from(TemplateBuilderSpec.parse(getProperty("MACHINE_SPEC")))
                .build();

        echo("Created template");

        template.getOptions()
                .inboundPorts(inboundPorts())
                .runScript(AdminAccess.standard())
                .securityGroups(getProperty("SECURITY_GROUP"));

        echo("Creating nodes");

        Set<Future> futures = new HashSet<Future>();
        echo("Created machines, waiting for startup (can take a few minutes)");

        for (int batch : calcBatches(delta)) {

            Set<? extends NodeMetadata> nodes = compute.createNodesInGroup("stabilizer-agent", batch, template);

            for (NodeMetadata node : nodes) {
                String privateIpAddress = node.getPrivateAddresses().iterator().next();
                String publicIpAddress = node.getPublicAddresses().iterator().next();

                echo("\t" + publicIpAddress + " LAUNCHED");
                appendText(publicIpAddress + "," + privateIpAddress + "\n", agentsFile);

                AgentAddress address = new AgentAddress(publicIpAddress,privateIpAddress);
                addresses.add(address);
            }

            for (NodeMetadata node : nodes) {
                Future f = executor.submit(new InstallNodeTask(node));
                futures.add(f);
            }
        }

        for (Future f : futures) {
            try {
                f.get();
            } catch (ExecutionException e) {
                log.severe("Failed provision", e);
                System.exit(1);
            }
        }

        long durationMs = System.currentTimeMillis() - startTimeMs;
        echo("Duration: " + secondsToHuman(TimeUnit.MILLISECONDS.toSeconds(durationMs)));
        echoImportant(format("Successfully provisioned %s %s machines",
                delta, getProperty("CLOUD_PROVIDER")));
    }

    private File hazelcastJarsDir;

    private void prepareHazelcastJars() {
        File tmpDir = new File(System.getProperty("java.io.tmpdir"));
        hazelcastJarsDir = new File(tmpDir, "hazelcastjars-" + UUID.randomUUID().toString());
        hazelcastJarsDir.mkdirs();

        String versionSpec = getProperty("HAZELCAST_VERSION_SPEC", "outofthebox");
        if (versionSpec.equals("outofthebox")) {
            log.info("Using Hazelcast version-spec: outofthebox");
        } else if (versionSpec.startsWith("path=")) {
            String path = versionSpec.substring(5);
            log.info("Using Hazelcast version-spec: path=" + path);
            File file = new File(path);
            if (!file.exists()) {
                log.severe("Directory :" + path + " does not exist");
                System.exit(1);
            }

            if (!file.isDirectory()) {
                log.severe("File :" + path + " is not a directory");
                System.exit(1);
            }

            bash(format("cp %s/* %s", path, hazelcastJarsDir.getAbsolutePath()));
        } else if (versionSpec.equals("none")) {
            log.info("Using Hazelcast version-spec: none");
            //we don't need to do anything
        } else if (versionSpec.startsWith("maven=")) {
            String version = versionSpec.substring(6);
            log.info("Using Hazelcast version-spec: maven=" + version);
            mavenRetrieve("hazelcast", version);
            mavenRetrieve("hazelcast-client", version);
        } else {
            log.severe("Unrecognized version spec:" + versionSpec);
            System.exit(1);
        }
    }

    private void mavenRetrieve(String artifact, String version) {
        File userhome = new File(System.getProperty("user.home"));
        File repositoryDir = Utils.toFile(userhome, ".m2", "repository");
        File artifactFile = Utils.toFile(repositoryDir, "com", "hazelcast",
                artifact, version, format("%s-%s.jar", artifact, version));
        if (artifactFile.exists()) {
            log.finest("Using artifact: " + artifactFile + " from local maven repository");
            bash(format("cp %s %s", artifactFile.getAbsolutePath(), hazelcastJarsDir.getAbsolutePath()));
        } else {
            log.finest("Artifact: " + artifactFile + " is not found in local maven repository, trying online one");

            String url;
            if (version.endsWith("-SNAPSHOT")) {
                String baseUrl = "https://oss.sonatype.org/content/repositories/snapshots";
                String mavenMetadataUrl = format("%s/com/hazelcast/%s/%s/maven-metadata.xml", baseUrl, artifact, version);
                log.finest("Loading: " + mavenMetadataUrl);
                String mavenMetadata = null;
                try {
                    mavenMetadata = Utils.getText(mavenMetadataUrl);
                } catch (FileNotFoundException e) {
                    log.severe("Failed to load " + artifact + "-" + version + ", because :"
                            + mavenMetadataUrl + " was not found");
                    System.exit(1);
                } catch (IOException e) {
                    log.severe("Could not load:" + mavenMetadataUrl);
                    System.exit(1);
                }

                log.finest(mavenMetadata);
                String timestamp = getTagValue(mavenMetadata, "timestamp");
                String buildnumber = getTagValue(mavenMetadata, "buildNumber");
                String shortVersion = version.replace("-SNAPSHOT", "");
                url = format("%s/com/hazelcast/%s/%s/%s-%s-%s-%s.jar",
                        baseUrl, artifact, version, artifact, shortVersion, timestamp, buildnumber);
            } else {
                String baseUrl = "http://repo1.maven.org/maven2";
                url = format("%s/com/hazelcast/%s/%s/%s-%s.jar", baseUrl, artifact, version, artifact, version);
            }

            bash(format("wget --no-verbose --directory-prefix=%s %s", hazelcastJarsDir.getAbsolutePath(), url));
        }
    }

    private String getTagValue(String mavenMetadata, String tag) {
        final Pattern pattern = Pattern.compile("<" + tag + ">(.+?)</" + tag + ">");
        final Matcher matcher = pattern.matcher(mavenMetadata);

        if (!matcher.find()) {
            throw new RuntimeException("Could not find " + tag + " in:" + mavenMetadata);
        }

        return matcher.group(1);
    }

    private class InstallNodeTask implements Runnable {
        private final String ip;

        InstallNodeTask(NodeMetadata node) {
            this.ip = node.getPrivateAddresses().iterator().next();
        }

        @Override
        public void run() {
            //install java if needed
            if (!"outofthebox".equals(getProperty("JDK_FLAVOR"))) {
                ssh(ip, "touch install-java.sh");
                ssh(ip, "chmod +x install-java.sh");
                scpToRemote(ip, getJavaInstallScript().getAbsolutePath(), "install-java.sh");
                ssh(ip, "bash install-java.sh");
                echo("\t" + ip + " JAVA INSTALLED");
            }

            installAgent(ip);
            echo("\t" + ip + " STABILIZER AGENT INSTALLED");

            startAgent(ip);
            echo("\t" + ip + " STABILIZER AGENT STARTED");
        }
    }

    private int[] calcBatches(int size) {
        List<Integer> batches = new LinkedList<Integer>();
        int batchSize = Integer.parseInt(getProperty("CLOUD_BATCH_SIZE"));
        while (size > 0) {
            int x = size >= batchSize ? batchSize : size;
            batches.add(x);
            size -= x;
        }

        int[] result = new int[batches.size()];
        for (int k = 0; k < result.length; k++) {
            result[k] = batches.get(k);
        }
        return result;
    }

    private ComputeService getComputeService() {
        //http://javadocs.jclouds.cloudbees.net/org/jclouds/compute/config/ComputeServiceProperties.html
        Properties overrides = new Properties();
        overrides.setProperty(POLL_INITIAL_PERIOD, getProperty("CLOUD_POLL_INITIAL_PERIOD"));
        overrides.setProperty(POLL_MAX_PERIOD, getProperty("CLOUD_POLL_MAX_PERIOD"));

        String credentials = getProperty("CLOUD_CREDENTIAL");
        File file = new File(credentials);
        if (file.exists()) {
            credentials = Utils.fileAsText(file);
        }

        return ContextBuilder.newBuilder(getProperty("CLOUD_PROVIDER"))
                .overrides(overrides)
                .credentials(getProperty("CLOUD_IDENTITY"), credentials)
                .modules(asList(new Log4JLoggingModule(), new SshjSshClientModule()))
                .buildView(ComputeServiceContext.class)
                .getComputeService();
    }

    private File getJavaInstallScript() {
        String flavor = getProperty("JDK_FLAVOR");
        String version = getProperty("JDK_VERSION");

        String script = "jdk-" + flavor + "-" + version + "-64.sh";
        return new File(JDK_INSTALL_DIR, script);
    }

    public void download() {
        echoImportant("Download artifacts of %s machines", addresses.size());

        bash("mkdir -p workers");

        for (AgentAddress address : addresses) {
            echo("Downloading from %s", address.publicAddress);

            String syncCommand = format("rsync  -av -e \"ssh %s\" %s@%s:hazelcast-stabilizer-%s/workers .",
                    getProperty("SSH_OPTIONS"), getProperty("USER"), address.publicAddress, getVersion());

            bash(syncCommand);
        }

        echoImportant("Finished Downloading Artifacts of %s machines", addresses.size());
    }

    public void clean() {
        echoImportant("Cleaning worker homes of %s machines", addresses.size());

        for (AgentAddress address : addresses) {
            echo("Cleaning %s", address.publicAddress);
            ssh(address.publicAddress, format("rm -fr hazelcast-stabilizer-%s/workers/*", getVersion()));
        }

        echoImportant("Finished cleaning worker homes of %s machines", addresses.size());
    }

    public void terminate() {
        terminate(Integer.MAX_VALUE);
    }

    public void terminate(int count) {
        if (count > addresses.size()) {
            count = addresses.size();
        }

        echoImportant(format("Terminating %s %s machines (can take some time)", count, getProperty("CLOUD_PROVIDER")));

        long startMs = System.currentTimeMillis();

        for (int batch : calcBatches(count)) {
            final Map<String, AgentAddress> terminateMap = new HashMap<String, AgentAddress>();
            for (AgentAddress address : addresses.subList(0, batch)) {
                terminateMap.put(address.publicAddress, address);
            }

            ComputeService computeService = getComputeService();
            computeService.destroyNodesMatching(
                    new Predicate<NodeMetadata>() {
                        @Override
                        public boolean apply(NodeMetadata nodeMetadata) {
                            for (String ip : nodeMetadata.getPublicAddresses()) {
                                AgentAddress address = terminateMap.remove(ip);
                                if (address != null) {
                                    echo(format("\t%s Terminating", ip));
                                    addresses.remove(address);
                                    return true;
                                }
                            }
                            return false;
                        }
                    }
            );
        }

        log.info("Updating " + agentsFile.getAbsolutePath());

        AgentsFile.save(agentsFile, addresses);

        long durationSeconds = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - startMs);
        echo("Duration: " + secondsToHuman(durationSeconds));
        echoImportant("Finished terminating %s %s machines, %s machines remaining.",
                count, CLOUD_PROVIDER, addresses.size());
    }


    private void bash(String command) {
        StringBuffer sout = new StringBuffer();

        log.finest("Executing bash command: " + command);

        try {
            // create a process for the shell
            ProcessBuilder pb = new ProcessBuilder("bash", "-c", command);
            pb = pb.redirectErrorStream(true);

            Process shell = pb.start();
            new StringBufferStreamGobbler(shell.getInputStream(), sout).start();

            // wait for the shell to finish and get the return code
            int shellExitStatus = shell.waitFor();

            if (shellExitStatus != 0) {
                echo("Failed to execute [%s]", command);
                log.severe(sout.toString());
                System.exit(1);
            } else {
                log.finest("Bash output: \n" + sout);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void scpToRemote(String ip, String src, String target) {
        String command = format("scp -r %s %s %s@%s:%s",
                getProperty("SSH_OPTIONS"), src, getProperty("USER"), ip, target);
        bash(command);
    }

    private void ssh(String ip, String command) {
        String sshCommand = format("ssh %s -q %s@%s \"%s\"",
                getProperty("SSH_OPTIONS"), getProperty("USER"), ip, command);
        bash(sshCommand);
    }

    private void sshQuiet(String ip, String command) {
        String sshCommand = format("ssh %s -q %s@%s \"%s\" || true",
                getProperty("SSH_OPTIONS"), getProperty("USER"), ip, command);
        bash(sshCommand);
    }

    private String getProperty(String name) {
        return (String) stabilizerProperties.get(name);
    }

    private String getProperty(String name, String defaultValue) {
        String value = (String) stabilizerProperties.get(name);
        if (value == null) {
            value = defaultValue;
        }
        return value;
    }

    private void echo(String s, Object... args) {
        log.info(s == null ? "null" : String.format(s, args));
    }

    private void echoImportant(String s, Object... args) {
        echo("==============================================================");
        echo(s, args);
        echo("==============================================================");
    }

    public static void main(String[] args) {
        try {
            ProvisionerCli cli = new ProvisionerCli();

            OptionSet options = cli.parser.parse(args);

            if (options.has(cli.helpSpec)) {
                cli.parser.printHelpOn(System.out);
                System.exit(0);
            }

            Provisioner provisioner = new Provisioner();
            for (OptionSpec spec : options.specs()) {
                if (spec.equals(cli.restartSpec)) {
                    provisioner.restart();
                    provisioner.startAgents();
                } else if (spec.equals(cli.killSpec)) {
                    provisioner.killAgents();
                } else if (spec.equals(cli.downloadSpec)) {
                    provisioner.download();
                } else if (spec.equals(cli.cleanSpec)) {
                    provisioner.clean();
                } else if (spec.equals(cli.terminateSpec)) {
                    provisioner.terminate();
                } else if (spec.equals(cli.scaleSpec)) {
                    int size = options.valueOf(cli.scaleSpec);
                    provisioner.scale(size);
                }
            }
            System.exit(0);
        } catch (OptionException e) {
            Utils.exitWithError(log, e.getMessage() + ". Use --help to get overview of the help options.");
        } catch (Throwable e) {
            log.severe(e);
            System.exit(1);
        }
    }
}
