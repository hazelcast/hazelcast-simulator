package com.hazelcast.simulator.provisioner;

import com.google.common.base.Predicate;
import com.hazelcast.simulator.common.AgentsFile;
import com.hazelcast.simulator.common.GitInfo;
import com.hazelcast.simulator.common.SimulatorProperties;
import com.hazelcast.simulator.protocol.registry.AgentData;
import com.hazelcast.simulator.protocol.registry.ComponentRegistry;
import com.hazelcast.simulator.utils.CommandLineExitException;
import com.hazelcast.simulator.utils.ThreadSpawner;
import com.hazelcast.simulator.utils.jars.HazelcastJARs;
import com.hazelcast.util.EmptyStatement;
import org.apache.log4j.Logger;
import org.jclouds.compute.ComputeService;
import org.jclouds.compute.domain.NodeMetadata;
import org.jclouds.compute.domain.Template;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static com.hazelcast.simulator.provisioner.ProvisionerUtils.getInitScriptFile;
import static com.hazelcast.simulator.utils.CommonUtils.exitWithError;
import static com.hazelcast.simulator.utils.CommonUtils.getSimulatorVersion;
import static com.hazelcast.simulator.utils.CommonUtils.sleepSeconds;
import static com.hazelcast.simulator.utils.ExecutorFactory.createFixedThreadPool;
import static com.hazelcast.simulator.utils.FileUtils.appendText;
import static com.hazelcast.simulator.utils.FileUtils.fileAsText;
import static com.hazelcast.simulator.utils.FileUtils.getSimulatorHome;
import static com.hazelcast.simulator.utils.FileUtils.rename;
import static com.hazelcast.simulator.utils.FormatUtils.secondsToHuman;
import static com.hazelcast.simulator.utils.HarakiriMonitorUtils.getStartHarakiriMonitorCommandOrNull;
import static com.hazelcast.simulator.utils.SimulatorUtils.loadComponentRegister;
import static java.lang.String.format;

public final class Provisioner {

    private static final int MACHINE_WARMUP_WAIT_SECONDS = 10;
    private static final int EXECUTOR_TERMINATION_TIMEOUT_SECONDS = 10;

    private static final String INDENTATION = "    ";

    private static final String SIMULATOR_HOME = getSimulatorHome().getAbsolutePath();

    private static final Logger LOGGER = Logger.getLogger(Provisioner.class);

    private final File agentsFile = new File(AgentsFile.NAME);
    private final ExecutorService executor = createFixedThreadPool(10, Provisioner.class);

    private final ComponentRegistry componentRegistry;

    private final SimulatorProperties props;
    private final Bash bash;

    private final File initScriptFile;
    private final HazelcastJARs hazelcastJARs;

    private ComputeService compute;

    public Provisioner(SimulatorProperties props) {
        this.componentRegistry = loadComponentRegister(agentsFile, false);
        this.props = props;
        this.bash = new Bash(props);

        this.initScriptFile = getInitScriptFile(SIMULATOR_HOME);
        this.hazelcastJARs = HazelcastJARs.newInstance(bash, props);
    }

    void scale(int size, boolean enterpriseEnabled) {
        ProvisionerUtils.ensureNotStaticCloudProvider(props, "scale");

        int agentSize = componentRegistry.agentCount();
        int delta = size - agentSize;
        if (delta == 0) {
            echo("Current number of machines: " + agentSize);
            echo("Desired number of machines: " + (agentSize + delta));
            echo("Ignoring spawn machines, desired number of machines already exists.");
        } else if (delta > 0) {
            scaleUp(delta, enterpriseEnabled);
        } else {
            scaleDown(-delta);
        }
    }

    void installSimulator(boolean enableEnterprise) {
        echoImportant("Installing Simulator on %s machines", componentRegistry.agentCount());
        hazelcastJARs.prepare(enableEnterprise);

        ThreadSpawner spawner = new ThreadSpawner("installSimulator", true);
        for (final AgentData agentData : componentRegistry.getAgents()) {
            spawner.spawn(new Runnable() {
                @Override
                public void run() {
                    echo("Installing Simulator on " + agentData.getPublicAddress());
                    installSimulator(agentData.getPublicAddress());
                }
            });
        }
        spawner.awaitCompletion();

        echoImportant("Installing Simulator on %s machines", componentRegistry.agentCount());
    }

    void listMachines() {
        echo("Provisioned machines (from " + AgentsFile.NAME + "):");
        String machines = fileAsText(agentsFile);
        echo(INDENTATION + machines);
    }

    void download(final String target) {
        echoImportant("Download artifacts of %s machines", componentRegistry.agentCount());
        bash.execute("mkdir -p " + target);

        ThreadSpawner spawner = new ThreadSpawner("download", true);

        final String baseCommand = "rsync --copy-links %s-avv -e \"ssh %s\" %s@%%s:%%s %s";
        final String sshOptions = props.get("SSH_OPTIONS", "");
        final String sshUser = props.getUser();

        // download Worker logs
        final String rsyncCommand = format(baseCommand, "", sshOptions, sshUser, target);
        final String workersPath = format("hazelcast-simulator-%s/workers/*", getSimulatorVersion());

        for (final AgentData agentData : componentRegistry.getAgents()) {
            spawner.spawn(new Runnable() {
                @Override
                public void run() {
                    echo("Downloading Worker logs from %s", agentData.getPublicAddress());
                    bash.executeQuiet(format(rsyncCommand, agentData.getPublicAddress(), workersPath));
                }
            });
        }

        // download Agent logs
        spawner.spawn(new Runnable() {
            @Override
            public void run() {
                String rsyncCommandSuffix = format(baseCommand, "--backup --suffix=-%s ", sshOptions, sshUser, target);
                File agentOut = new File(target + "/agent.out");
                File agentErr = new File(target + "/agent.err");

                for (AgentData agentData : componentRegistry.getAgents()) {
                    String agentAddress = agentData.getPublicAddress();
                    echo("Downloading Agent logs from %s", agentAddress);

                    bash.executeQuiet(format(rsyncCommandSuffix, agentAddress, agentAddress, "agent.out"));
                    bash.executeQuiet(format(rsyncCommandSuffix, agentAddress, agentAddress, "agent.err"));

                    rename(agentOut, new File(target + "/" + agentAddress + "-agent.out"));
                    rename(agentErr, new File(target + "/" + agentAddress + "-agent.err"));
                }

            }
        });

        spawner.awaitCompletion();
        echoImportant("Finished downloading artifacts of %s machines", componentRegistry.agentCount());
    }

    void clean() {
        echoImportant("Cleaning worker homes of %s machines", componentRegistry.agentCount());
        final String cleanCommand = format("rm -fr hazelcast-simulator-%s/workers/*", getSimulatorVersion());

        ThreadSpawner spawner = new ThreadSpawner("clean", true);
        for (final AgentData agentData : componentRegistry.getAgents()) {
            spawner.spawn(new Runnable() {
                @Override
                public void run() {
                    echo("Cleaning %s", agentData.getPublicAddress());
                    bash.ssh(agentData.getPublicAddress(), cleanCommand);
                }
            });
        }
        spawner.awaitCompletion();

        echoImportant("Finished cleaning worker homes of %s machines", componentRegistry.agentCount());
    }

    void killJavaProcesses() {
        echoImportant("Killing %s Java processes", componentRegistry.agentCount());

        ThreadSpawner spawner = new ThreadSpawner("killJavaProcesses", true);
        for (final AgentData agentData : componentRegistry.getAgents()) {
            spawner.spawn(new Runnable() {
                @Override
                public void run() {
                    echo("Killing Java processes on %s", agentData.getPublicAddress());
                    bash.killAllJavaProcesses(agentData.getPublicAddress());
                }
            });
        }
        spawner.awaitCompletion();

        echoImportant("Successfully killed %s Java processes", componentRegistry.agentCount());
    }

    void terminate() {
        ProvisionerUtils.ensureNotStaticCloudProvider(props, "terminate");

        scaleDown(Integer.MAX_VALUE);
    }

    void shutdown() {
        echo("Shutting down Provisioner...");

        // shutdown thread pool
        try {
            executor.shutdown();
            executor.awaitTermination(EXECUTOR_TERMINATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (Exception ignored) {
            EmptyStatement.ignore(ignored);
        }

        // shutdown compute service (which holds another thread pool)
        if (compute != null) {
            compute.getContext().close();
        }

        echo("Done!");
    }

    private void scaleUp(int delta, boolean enterpriseEnabled) {
        echoImportant("Provisioning %s %s machines", delta, props.get("CLOUD_PROVIDER"));
        echo("Current number of machines: " + componentRegistry.agentCount());
        echo("Desired number of machines: " + (componentRegistry.agentCount() + delta));
        String groupName = props.get("GROUP_NAME", "simulator-agent");
        echo("GroupName: " + groupName);
        echo("Username: " + props.getUser());

        LOGGER.info("Using init script: " + initScriptFile.getAbsolutePath());

        long started = System.nanoTime();

        String jdkFlavor = props.get("JDK_FLAVOR", "outofthebox");
        if ("outofthebox".equals(jdkFlavor)) {
            LOGGER.info("JDK spec: outofthebox");
        } else {
            String jdkVersion = props.get("JDK_VERSION", "7");
            LOGGER.info(format("JDK spec: %s %s", jdkFlavor, jdkVersion));
        }

        hazelcastJARs.prepare(enterpriseEnabled);

        compute = new ComputeServiceBuilder(props).build();
        echo("Created compute");

        Template template = new TemplateBuilder(compute, props).build();

        String startHarakiriMonitorCommand = getStartHarakiriMonitorCommandOrNull(props);

        try {
            echo("Creating machines (can take a few minutes)...");
            Set<Future> futures = new HashSet<Future>();
            for (int batch : ProvisionerUtils.calcBatches(props, delta)) {
                Set<? extends NodeMetadata> nodes = compute.createNodesInGroup(groupName, batch, template);
                for (NodeMetadata node : nodes) {
                    String privateIpAddress = node.getPrivateAddresses().iterator().next();
                    String publicIpAddress = node.getPublicAddresses().iterator().next();

                    echo(INDENTATION + publicIpAddress + " LAUNCHED");
                    appendText(publicIpAddress + "," + privateIpAddress + "\n", agentsFile);

                    componentRegistry.addAgent(publicIpAddress, privateIpAddress);
                }

                for (NodeMetadata node : nodes) {
                    String publicIpAddress = node.getPublicAddresses().iterator().next();
                    Future future = executor.submit(new InstallNodeTask(publicIpAddress, startHarakiriMonitorCommand));
                    futures.add(future);
                }
            }

            for (Future future : futures) {
                future.get();
            }
        } catch (Exception e) {
            throw new CommandLineExitException("Failed to provision machines: " + e.getMessage());
        }

        echo(format("Pausing for machine warmup... (%d sec)", MACHINE_WARMUP_WAIT_SECONDS));
        sleepSeconds(MACHINE_WARMUP_WAIT_SECONDS);

        echo("Duration: " + secondsToHuman(TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - started)));
        echoImportant(format("Successfully provisioned %s %s machines", delta, props.get("CLOUD_PROVIDER")));
    }

    private void scaleDown(int count) {
        if (count > componentRegistry.agentCount()) {
            count = componentRegistry.agentCount();
        }

        echoImportant(format("Terminating %s %s machines (can take some time)", count, props.get("CLOUD_PROVIDER")));
        echo("Current number of machines: " + componentRegistry.agentCount());
        echo("Desired number of machines: " + (componentRegistry.agentCount() - count));

        long started = System.nanoTime();

        compute = new ComputeServiceBuilder(props).build();

        int destroyedCount = 0;
        for (int batchSize : ProvisionerUtils.calcBatches(props, count)) {
            Map<String, AgentData> terminateMap = new HashMap<String, AgentData>();
            for (AgentData agentData : componentRegistry.getAgents(batchSize)) {
                terminateMap.put(agentData.getPublicAddress(), agentData);
            }
            destroyedCount += destroyNodes(compute, terminateMap);
        }

        LOGGER.info("Updating " + agentsFile.getAbsolutePath());
        AgentsFile.save(agentsFile, componentRegistry);

        echo("Duration: " + secondsToHuman(TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - started)));
        echoImportant("Terminated %s of %s, remaining=%s", destroyedCount, count, componentRegistry.agentCount());

        if (destroyedCount != count) {
            throw new IllegalStateException("Terminated " + destroyedCount + " of " + count
                    + "\n1) You are trying to terminate physical hardware that you own (unsupported feature)"
                    + "\n2) If and only if you are using AWS our Harakiri Monitor might have terminated them"
                    + "\n3) You have not payed you bill and your instances have been terminated by your cloud provider"
                    + "\n4) You have terminated your own instances (perhaps via some console interface)"
                    + "\n5) Someone else has terminated your instances"
                    + "\nPlease try again!");
        }
    }

    private void installSimulator(String ip) {
        bash.ssh(ip, format("mkdir -p hazelcast-simulator-%s/lib/", getSimulatorVersion()));

        // first we delete the old lib files to prevent different versions of the same JAR to bite us
        bash.sshQuiet(ip, format("rm -f hazelcast-simulator-%s/lib/*", getSimulatorVersion()));

        // upload Simulator JARs
        bash.uploadToAgentSimulatorDir(ip, SIMULATOR_HOME + "/lib/simulator-*", "lib");
        bash.uploadToAgentSimulatorDir(ip, SIMULATOR_HOME + "/lib/probes-*", "lib");
        bash.uploadToAgentSimulatorDir(ip, SIMULATOR_HOME + "/lib/tests-*", "lib");
        bash.uploadToAgentSimulatorDir(ip, SIMULATOR_HOME + "/lib/utils-*", "lib");

        // we don't copy all JARs to the agent to increase upload speed, e.g. YourKit is uploaded on demand by the Coordinator
        bash.uploadToAgentSimulatorDir(ip, SIMULATOR_HOME + "/lib/cache-api*", "lib");
        bash.uploadToAgentSimulatorDir(ip, SIMULATOR_HOME + "/lib/commons-codec*", "lib");
        bash.uploadToAgentSimulatorDir(ip, SIMULATOR_HOME + "/lib/commons-lang3*", "lib");
        bash.uploadToAgentSimulatorDir(ip, SIMULATOR_HOME + "/lib/gson-*", "lib");
        bash.uploadToAgentSimulatorDir(ip, SIMULATOR_HOME + "/lib/guava-*", "lib");
        bash.uploadToAgentSimulatorDir(ip, SIMULATOR_HOME + "/lib/jopt*", "lib");
        bash.uploadToAgentSimulatorDir(ip, SIMULATOR_HOME + "/lib/junit*", "lib");
        bash.uploadToAgentSimulatorDir(ip, SIMULATOR_HOME + "/lib/HdrHistogram-*", "lib");
        bash.uploadToAgentSimulatorDir(ip, SIMULATOR_HOME + "/lib/log4j*", "lib");
        bash.uploadToAgentSimulatorDir(ip, SIMULATOR_HOME + "/lib/netty-*", "lib");

        // upload remaining files
        bash.uploadToAgentSimulatorDir(ip, SIMULATOR_HOME + "/bin/", "bin");
        bash.uploadToAgentSimulatorDir(ip, SIMULATOR_HOME + "/conf/", "conf");
        bash.uploadToAgentSimulatorDir(ip, SIMULATOR_HOME + "/jdk-install/", "jdk-install");
        bash.uploadToAgentSimulatorDir(ip, SIMULATOR_HOME + "/tests/", "tests");
        bash.uploadToAgentSimulatorDir(ip, SIMULATOR_HOME + "/user-lib/", "user-lib/");

        // upload Hazelcast JARs
        hazelcastJARs.purge(ip);
        hazelcastJARs.upload(ip, SIMULATOR_HOME);

        String initScript = loadInitScript();
        bash.ssh(ip, initScript);
    }

    private String loadInitScript() {
        String initScript = fileAsText(initScriptFile);

        initScript = initScript.replaceAll(Pattern.quote("${user}"), props.getUser());
        initScript = initScript.replaceAll(Pattern.quote("${version}"), getSimulatorVersion());

        return initScript;
    }

    private int destroyNodes(ComputeService compute, final Map<String, AgentData> terminateMap) {
        Set destroyedSet = compute.destroyNodesMatching(new Predicate<NodeMetadata>() {
            @Override
            public boolean apply(NodeMetadata nodeMetadata) {
                for (String publicAddress : nodeMetadata.getPublicAddresses()) {
                    AgentData agentData = terminateMap.remove(publicAddress);
                    if (agentData != null) {
                        echo(format("    Terminating instance %s", publicAddress));
                        componentRegistry.removeAgent(agentData);
                        return true;
                    }
                }
                return false;
            }
        });
        return destroyedSet.size();
    }

    private void echo(String s, Object... args) {
        LOGGER.info(s == null ? "null" : String.format(s, args));
    }

    private void echoImportant(String s, Object... args) {
        echo("==============================================================");
        echo(s, args);
        echo("==============================================================");
    }

    private final class InstallNodeTask implements Runnable {

        private final String ip;
        private final String startHarakiriMonitorCommand;

        private InstallNodeTask(String ip, String startHarakiriMonitorCommand) {
            this.ip = ip;
            this.startHarakiriMonitorCommand = startHarakiriMonitorCommand;
        }

        @Override
        public void run() {
            // install Java if needed
            if (!"outofthebox".equals(props.get("JDK_FLAVOR"))) {
                echo(INDENTATION + ip + " JAVA INSTALLATION STARTED...");
                bash.scpToRemote(ip, getJavaSupportScript(), "jdk-support.sh");
                bash.scpToRemote(ip, getJavaInstallScript(), "install-java.sh");
                bash.ssh(ip, "bash install-java.sh");
                echo(INDENTATION + ip + " JAVA INSTALLED");
            }

            echo(INDENTATION + ip + " SIMULATOR INSTALLATION STARTED...");
            installSimulator(ip);
            echo(INDENTATION + ip + " SIMULATOR INSTALLED");

            if (startHarakiriMonitorCommand != null) {
                bash.ssh(ip, startHarakiriMonitorCommand);
                echo(INDENTATION + ip + " HARAKIRI MONITOR STARTED");
            }
        }

        private File getJavaInstallScript() {
            String flavor = props.get("JDK_FLAVOR");
            String version = props.get("JDK_VERSION");

            String script = "jdk-" + flavor + "-" + version + "-64.sh";
            File scriptDir = new File(SIMULATOR_HOME, "jdk-install");
            return new File(scriptDir, script);
        }

        private File getJavaSupportScript() {
            File scriptDir = new File(SIMULATOR_HOME, "jdk-install");
            return new File(scriptDir, "jdk-support.sh");
        }

    }

    public static void main(String[] args) {
        try {
            LOGGER.info("Hazelcast Simulator Provisioner");
            LOGGER.info(format("Version: %s, Commit: %s, Build Time: %s", getSimulatorVersion(),
                    GitInfo.getCommitIdAbbrev(), GitInfo.getBuildTime()));
            LOGGER.info(format("SIMULATOR_HOME: %s", SIMULATOR_HOME));

            Provisioner provisioner = ProvisionerCli.init(args);
            ProvisionerCli.run(args, provisioner);
        } catch (Exception e) {
            exitWithError(LOGGER, "Could not provision machines", e);
        }
    }
}
