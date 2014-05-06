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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

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
    private final Bash bash;
    private final HazelcastJars hazelcastJars;

    public Provisioner() throws Exception {
        log.info("Hazelcast Stabilizer Provisioner");
        log.info(format("Version: %s", getVersion()));
        log.info(format("STABILIZER_HOME: %s", STABILIZER_HOME));

        if (!agentsFile.exists()) {
            agentsFile.createNewFile();
        }

        addresses.addAll(AgentsFile.load(agentsFile));
        bash = new Bash(stabilizerProperties);
        hazelcastJars = new HazelcastJars(bash,    getProperty("HAZELCAST_VERSION_SPEC", "outofthebox"));
    }

    private Properties loadStabilizerProperties() {
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
        bash.sshQuiet(ip, format("rm -fr hazelcast-stabilizer-%s/lib", VERSION));

        //then we copy the stabilizer directory
        bash.scpToRemote(ip, STABILIZER_HOME, "");

        String versionSpec = getProperty("HAZELCAST_VERSION_SPEC", "outofthebox");
        if (!versionSpec.equals("outofthebox") && !versionSpec.endsWith("none")) {
            //remove the hazelcast jars, they will be copied from the 'hazelcastJarsDir'.
            bash.ssh(ip, format("rm hazelcast-stabilizer-%s/lib/hazelcast-*.jar", VERSION));
            //copy the actual hazelcast jars that are going to be used by the worker.
            bash.scpToRemote(ip, hazelcastJars.getAbsolutePath() + "/*.jar", format("hazelcast-stabilizer-%s/lib", VERSION));
        }
    }

    public void startAgents() {
        echoImportant("Starting %s Agents", addresses.size());

        for (AgentAddress address : addresses) {
            echo("Killing Agent %s", address.publicAddress);
            bash.ssh(address.publicAddress, "killall -9 java || true");
        }

        for (AgentAddress address : addresses) {
            echo("Starting Agent %s", address.publicAddress);
            bash.ssh(address.publicAddress, format("nohup hazelcast-stabilizer-%s/bin/agent >agent.out &", VERSION));
        }

        echoImportant("Successfully started %s Agents", addresses.size());
    }

    void startAgent(String ip) {
        bash.ssh(ip, "killall -9 java || true");
        bash.ssh(ip, format("nohup hazelcast-stabilizer-%s/bin/agent >agent.out &", getVersion()));
    }

    void killAgents() {
        echoImportant("Killing %s Agents", addresses.size());

        for (AgentAddress address : addresses) {
            echo("Killing Agent, %s", address.publicAddress);
            bash.ssh(address.publicAddress, "killall -9 java || true");
        }

        echoImportant("Successfully killed %s Agents", addresses.size());
    }

    public void restart() {
        hazelcastJars.prepare();
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

        hazelcastJars.prepare();

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

        String groupName = getProperty("GROUP_NAME","stabilizer-agent");

        for (int batch : calcBatches(delta)) {

            Set<? extends NodeMetadata> nodes = compute.createNodesInGroup(groupName, batch, template);

            for (NodeMetadata node : nodes) {
                String privateIpAddress = node.getPrivateAddresses().iterator().next();
                String publicIpAddress = node.getPublicAddresses().iterator().next();

                echo("\t" + publicIpAddress + " LAUNCHED");
                appendText(publicIpAddress + "," + privateIpAddress + "\n", agentsFile);

                AgentAddress address = new AgentAddress(publicIpAddress, privateIpAddress);
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

    private class InstallNodeTask implements Runnable {
        private final String ip;

        InstallNodeTask(NodeMetadata node) {
            this.ip = node.getPrivateAddresses().iterator().next();
        }

        @Override
        public void run() {
            //install java if needed
            if (!"outofthebox".equals(getProperty("JDK_FLAVOR"))) {
                bash.ssh(ip, "touch install-java.sh");
                bash.ssh(ip, "chmod +x install-java.sh");
                bash.scpToRemote(ip, getJavaInstallScript().getAbsolutePath(), "install-java.sh");
                bash.ssh(ip, "bash install-java.sh");
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

        bash.bash("mkdir -p workers");

        for (AgentAddress address : addresses) {
            echo("Downloading from %s", address.publicAddress);

            String syncCommand = format("rsync  -av -e \"ssh %s\" %s@%s:hazelcast-stabilizer-%s/workers .",
                    getProperty("SSH_OPTIONS"), getProperty("USER"), address.publicAddress, getVersion());

            bash.bash(syncCommand);
        }

        echoImportant("Finished Downloading Artifacts of %s machines", addresses.size());
    }

    public void clean() {
        echoImportant("Cleaning worker homes of %s machines", addresses.size());

        for (AgentAddress address : addresses) {
            echo("Cleaning %s", address.publicAddress);
            bash.ssh(address.publicAddress, format("rm -fr hazelcast-stabilizer-%s/workers/*", getVersion()));
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
                            for (String publicAddress : nodeMetadata.getPublicAddresses()) {
                                AgentAddress address = terminateMap.remove(publicAddress);
                                if (address != null) {
                                    echo(format("\t%s Terminating", publicAddress));
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
