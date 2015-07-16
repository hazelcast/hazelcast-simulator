package com.hazelcast.simulator.provisioner;

import com.google.common.base.Predicate;
import com.hazelcast.simulator.common.AgentAddress;
import com.hazelcast.simulator.common.AgentsFile;
import com.hazelcast.simulator.common.GitInfo;
import com.hazelcast.simulator.common.SimulatorProperties;
import com.hazelcast.simulator.provisioner.git.BuildSupport;
import com.hazelcast.simulator.provisioner.git.GitSupport;
import com.hazelcast.simulator.provisioner.git.HazelcastJARFinder;
import com.hazelcast.simulator.utils.CommandLineExitException;
import com.hazelcast.util.EmptyStatement;
import org.apache.log4j.Logger;
import org.jclouds.compute.ComputeService;
import org.jclouds.compute.domain.NodeMetadata;
import org.jclouds.compute.domain.Template;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static com.hazelcast.simulator.utils.CloudProviderUtils.isEC2;
import static com.hazelcast.simulator.utils.CloudProviderUtils.isStatic;
import static com.hazelcast.simulator.utils.CommonUtils.exitWithError;
import static com.hazelcast.simulator.utils.CommonUtils.getSimulatorVersion;
import static com.hazelcast.simulator.utils.CommonUtils.secondsToHuman;
import static com.hazelcast.simulator.utils.CommonUtils.sleepSeconds;
import static com.hazelcast.simulator.utils.ExecutorFactory.createFixedThreadPool;
import static com.hazelcast.simulator.utils.FileUtils.appendText;
import static com.hazelcast.simulator.utils.FileUtils.ensureExistingFile;
import static com.hazelcast.simulator.utils.FileUtils.fileAsText;
import static com.hazelcast.simulator.utils.FileUtils.getSimulatorHome;
import static java.lang.String.format;

public final class Provisioner {

    private static final int MACHINE_WARMUP_WAIT_SECONDS = 10;
    private static final int EXECUTOR_TERMINATION_TIMEOUT_SECONDS = 10;

    private static final String SIMULATOR_HOME = getSimulatorHome().getAbsolutePath();
    private static final String CONF_DIR = SIMULATOR_HOME + "/conf";

    private static final Logger LOGGER = Logger.getLogger(Provisioner.class);

    final SimulatorProperties props = new SimulatorProperties();

    private final File agentsFile = new File(AgentsFile.NAME);
    private final List<AgentAddress> addresses = Collections.synchronizedList(new LinkedList<AgentAddress>());
    // big number of threads, but they are used to offload SSH tasks, so there is no load on this machine
    private final ExecutorService executor = createFixedThreadPool(10, Provisioner.class);

    private Bash bash;
    private HazelcastJars hazelcastJars;
    private File initScript;
    private ComputeService compute;

    void init() {
        ensureExistingFile(agentsFile);
        addresses.addAll(AgentsFile.load(agentsFile));
        bash = new Bash(props);

        GitSupport gitSupport = createGitSupport();
        hazelcastJars = new HazelcastJars(bash, gitSupport, props.getHazelcastVersionSpec());

        initScript = new File("init.sh");
        if (!initScript.exists()) {
            initScript = new File(CONF_DIR + "/init.sh");
        }
    }

    private GitSupport createGitSupport() {
        String mvnExec = props.get("MVN_EXECUTABLE");
        BuildSupport buildSupport = new BuildSupport(bash, new HazelcastJARFinder(), mvnExec);
        String gitBuildDirectory = props.get("GIT_BUILD_DIR");
        String customGitRepositories = props.get("GIT_CUSTOM_REPOSITORIES");
        return new GitSupport(buildSupport, customGitRepositories, gitBuildDirectory);
    }

    void startAgents() {
        echoImportant("Starting %s Agents", addresses.size());

        for (AgentAddress address : addresses) {
            startAgent(address.publicAddress);
        }

        echoImportant("Successfully started agents on %s boxes", addresses.size());
    }

    void killAgents() {
        echoImportant("Killing %s Agents", addresses.size());

        for (AgentAddress address : addresses) {
            echo("Killing Agent, %s", address.publicAddress);
            bash.ssh(address.publicAddress, "killall -9 java || true");
        }

        echoImportant("Successfully killed %s Agents", addresses.size());
    }

    void restart(boolean enableEnterprise) {
        echoImportant("Restarting %s Agents", addresses.size());

        hazelcastJars.prepare(enableEnterprise);
        for (AgentAddress address : addresses) {
            echo("Installing agent: " + address.publicAddress);
            installAgent(address.publicAddress);
        }

        echoImportant("Restarting %s Agents", addresses.size());
    }

    void scale(int size, boolean enterpriseEnabled) {
        ensureNotStaticCloudProvider("scale");

        int delta = size - addresses.size();
        if (delta == 0) {
            echo("Current number of machines: " + addresses.size());
            echo("Desired number of machines: " + (addresses.size() + delta));
            echo("Ignoring spawn machines, desired number of machines already exists.");
        } else if (delta > 0) {
            scaleUp(delta, enterpriseEnabled);
        } else {
            scaleDown(-delta);
        }
    }

    void terminate() {
        ensureNotStaticCloudProvider("terminate");

        scaleDown(Integer.MAX_VALUE);
    }

    void ensureNotStaticCloudProvider(String action) {
        if (isStatic(props.get("CLOUD_PROVIDER"))) {
            throw new CommandLineExitException(format("Cannot execute '%s' in static setup", action));
        }
    }

    void listAgents() {
        echo("Running Agents (from " + AgentsFile.NAME + "):");
        String agents = fileAsText(agentsFile);
        echo("    " + agents);
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

    void download(String dir) {
        echoImportant("Download artifacts of %s machines", addresses.size());

        bash.execute("mkdir -p " + dir);

        for (AgentAddress address : addresses) {
            echo("Downloading from %s", address.publicAddress);

            String syncCommand = format("rsync --copy-links  -avv -e \"ssh %s\" %s@%s:hazelcast-simulator-%s/workers/* " + dir,
                    props.get("SSH_OPTIONS", ""), props.getUser(), address.publicAddress, getSimulatorVersion());

            bash.executeQuiet(syncCommand);
        }

        echoImportant("Finished Downloading Artifacts of %s machines", addresses.size());
    }

    void clean() {
        echoImportant("Cleaning worker homes of %s machines", addresses.size());

        for (AgentAddress address : addresses) {
            echo("Cleaning %s", address.publicAddress);
            bash.ssh(address.publicAddress, format("rm -fr hazelcast-simulator-%s/workers/*", getSimulatorVersion()));
        }

        echoImportant("Finished cleaning worker homes of %s machines", addresses.size());
    }

    private void scaleUp(int delta, boolean enterpriseEnabled) {
        echoImportant("Provisioning %s %s machines", delta, props.get("CLOUD_PROVIDER"));
        echo("Current number of machines: " + addresses.size());
        echo("Desired number of machines: " + (addresses.size() + delta));
        String groupName = props.get("GROUP_NAME", "simulator-agent");
        echo("GroupName: " + groupName);
        echo("Username: " + props.getUser());

        LOGGER.info("Using init script:" + initScript.getAbsolutePath());

        long started = System.nanoTime();

        String jdkFlavor = props.get("JDK_FLAVOR", "outofthebox");
        if ("outofthebox".equals(jdkFlavor)) {
            LOGGER.info("JDK spec: outofthebox");
        } else {
            String jdkVersion = props.get("JDK_VERSION", "7");
            LOGGER.info(format("JDK spec: %s %s", jdkFlavor, jdkVersion));
        }

        hazelcastJars.prepare(enterpriseEnabled);

        compute = new ComputeServiceBuilder(props).build();
        echo("Created compute");

        Template template = new TemplateBuilder(compute, props).build();

        try {
            echo("Creating machines... (can take a few minutes)");
            Set<Future> futures = new HashSet<Future>();
            for (int batch : calcBatches(delta)) {
                Set<? extends NodeMetadata> nodes = compute.createNodesInGroup(groupName, batch, template);
                for (NodeMetadata node : nodes) {
                    String privateIpAddress = node.getPrivateAddresses().iterator().next();
                    String publicIpAddress = node.getPublicAddresses().iterator().next();

                    echo("    " + publicIpAddress + " LAUNCHED");
                    appendText(publicIpAddress + "," + privateIpAddress + "\n", agentsFile);

                    AgentAddress address = new AgentAddress(publicIpAddress, privateIpAddress);
                    addresses.add(address);
                }

                for (NodeMetadata node : nodes) {
                    String publicIpAddress = node.getPublicAddresses().iterator().next();
                    Future future = executor.submit(new InstallNodeTask(publicIpAddress));
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
        if (count > addresses.size()) {
            count = addresses.size();
        }

        echoImportant(format("Terminating %s %s machines (can take some time)", count, props.get("CLOUD_PROVIDER")));
        echo("Current number of machines: " + addresses.size());
        echo("Desired number of machines: " + (addresses.size() - count));

        long started = System.nanoTime();

        compute = new ComputeServiceBuilder(props).build();

        int destroyedCount = 0;
        for (int batchSize : calcBatches(count)) {
            final Map<String, AgentAddress> terminateMap = new HashMap<String, AgentAddress>();
            for (AgentAddress address : addresses.subList(0, batchSize)) {
                terminateMap.put(address.publicAddress, address);
            }
            destroyedCount += destroyNodes(compute, terminateMap);
        }

        LOGGER.info("Updating " + agentsFile.getAbsolutePath());

        AgentsFile.save(agentsFile, addresses);

        echo("Duration: " + secondsToHuman(TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - started)));
        echoImportant("Terminated %s of %s, remaining=%s", destroyedCount, count, addresses.size());

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

    private int[] calcBatches(int size) {
        List<Integer> batches = new LinkedList<Integer>();
        int batchSize = Integer.parseInt(props.get("CLOUD_BATCH_SIZE"));
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

    private void installAgent(String ip) {
        bash.ssh(ip, format("mkdir -p hazelcast-simulator-%s/lib/", getSimulatorVersion()));

        // first we delete the old lib files to prevent different versions of the same JAR to bite us
        bash.sshQuiet(ip, format("rm -f hazelcast-simulator-%s/lib/*", getSimulatorVersion()));

        // upload Simulator JARs
        bash.uploadToAgentSimulatorDir(ip, SIMULATOR_HOME + "/lib/simulator-*", "lib");
        bash.uploadToAgentSimulatorDir(ip, SIMULATOR_HOME + "/lib/probes-*", "lib");
        bash.uploadToAgentSimulatorDir(ip, SIMULATOR_HOME + "/lib/tests-*", "lib");
        bash.uploadToAgentSimulatorDir(ip, SIMULATOR_HOME + "/lib/utils-*", "lib");

        // upload Hazelcast JARs
        bash.uploadToAgentSimulatorDir(ip, SIMULATOR_HOME + "/lib/hazelcast*", "lib");

        // we don't copy all JARs to the agent to increase upload speed, e.g. YourKit is uploaded on demand by the Coordinator
        bash.uploadToAgentSimulatorDir(ip, SIMULATOR_HOME + "/lib/cache-api*", "lib");
        bash.uploadToAgentSimulatorDir(ip, SIMULATOR_HOME + "/lib/commons-codec*", "lib");
        bash.uploadToAgentSimulatorDir(ip, SIMULATOR_HOME + "/lib/commons-lang3*", "lib");
        bash.uploadToAgentSimulatorDir(ip, SIMULATOR_HOME + "/lib/guava-*", "lib");
        bash.uploadToAgentSimulatorDir(ip, SIMULATOR_HOME + "/lib/jopt*", "lib");
        bash.uploadToAgentSimulatorDir(ip, SIMULATOR_HOME + "/lib/junit*", "lib");
        bash.uploadToAgentSimulatorDir(ip, SIMULATOR_HOME + "/lib/HdrHistogram-*", "lib");
        bash.uploadToAgentSimulatorDir(ip, SIMULATOR_HOME + "/lib/log4j*", "lib");

        // upload remaining files
        bash.uploadToAgentSimulatorDir(ip, SIMULATOR_HOME + "/bin/", "bin");
        bash.uploadToAgentSimulatorDir(ip, SIMULATOR_HOME + "/conf/", "conf");
        bash.uploadToAgentSimulatorDir(ip, SIMULATOR_HOME + "/jdk-install/", "jdk-install");
        bash.uploadToAgentSimulatorDir(ip, SIMULATOR_HOME + "/tests/", "tests");
        bash.uploadToAgentSimulatorDir(ip, SIMULATOR_HOME + "/user-lib/", "user-lib/");

        String script = loadInitScript();
        bash.ssh(ip, script);

        String versionSpec = props.getHazelcastVersionSpec();
        if (!versionSpec.equals("outofthebox")) {
            // TODO: in the future we can improve this (we upload the Hazelcast JARs, to delete them again)

            // remove the Hazelcast JARs, they will be copied from the 'hazelcastJarsDir'
            bash.ssh(ip, format("rm -fr hazelcast-simulator-%s/lib/hazelcast-*.jar", getSimulatorVersion()));

            if (!versionSpec.endsWith("bringmyown")) {
                // upload the actual Hazelcast JARs that are going to be used by the worker
                bash.scpToRemote(ip, hazelcastJars.getAbsolutePath() + "/*.jar",
                        format("hazelcast-simulator-%s/lib", getSimulatorVersion()));
            }
        }
    }

    private String loadInitScript() {
        String script = fileAsText(initScript);

        script = script.replaceAll(Pattern.quote("${user}"), props.getUser());
        script = script.replaceAll(Pattern.quote("${version}"), getSimulatorVersion());

        return script;
    }

    private void startAgent(String ip) {
        echo("Killing Agent on: %s", ip);
        bash.ssh(ip, "killall -9 java || true");

        echo("Starting Agent on: %s", ip);
        if (isEC2(props.get("CLOUD_PROVIDER"))) {
            bash.ssh(ip, format(
                    "nohup hazelcast-simulator-%s/bin/agent --cloudProvider %s --cloudIdentity %s --cloudCredential %s "
                            + "> agent.out 2> agent.err < /dev/null &",
                    getSimulatorVersion(),
                    props.get("CLOUD_PROVIDER"),
                    props.get("CLOUD_IDENTITY"),
                    props.get("CLOUD_CREDENTIAL")));
        } else {
            bash.ssh(ip, format(
                    "nohup hazelcast-simulator-%s/bin/agent > agent.out 2> agent.err < /dev/null &",
                    getSimulatorVersion()));
        }
    }

    private int destroyNodes(ComputeService compute, final Map<String, AgentAddress> terminateMap) {
        Set destroyedSet = compute.destroyNodesMatching(new Predicate<NodeMetadata>() {
            @Override
            public boolean apply(NodeMetadata nodeMetadata) {
                for (String publicAddress : nodeMetadata.getPublicAddresses()) {
                    AgentAddress address = terminateMap.remove(publicAddress);
                    if (address != null) {
                        echo(format("    %s Terminating", publicAddress));
                        addresses.remove(address);
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

        private InstallNodeTask(String ip) {
            this.ip = ip;
        }

        @Override
        public void run() {
            // install Java if needed
            if (!"outofthebox".equals(props.get("JDK_FLAVOR"))) {
                bash.scpToRemote(ip, getJavaSupportScript(), "jdk-support.sh");
                bash.scpToRemote(ip, getJavaInstallScript(), "install-java.sh");
                bash.ssh(ip, "bash install-java.sh");
                echo("    " + ip + " JAVA INSTALLED");
            }

            installAgent(ip);
            echo("    " + ip + " SIMULATOR AGENT INSTALLED");

            startAgent(ip);
            echo("    " + ip + " SIMULATOR AGENT STARTED");
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
            LOGGER.info(format("Version: %s, Commit: %s, Build Time: %s", getSimulatorVersion(), GitInfo.getCommitIdAbbrev(),
                    GitInfo.getBuildTime()));
            LOGGER.info(format("SIMULATOR_HOME: %s", SIMULATOR_HOME));

            Provisioner provisioner = new Provisioner();
            ProvisionerCli cli = new ProvisionerCli(provisioner, args);
            cli.init();

            cli.run();
        } catch (Exception e) {
            exitWithError(LOGGER, "Could not provision machines", e);
        }
    }
}
