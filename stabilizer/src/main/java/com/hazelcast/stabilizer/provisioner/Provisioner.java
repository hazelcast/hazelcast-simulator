package com.hazelcast.stabilizer.provisioner;

import com.google.common.base.Predicate;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.Utils;
import com.hazelcast.stabilizer.common.AgentAddress;
import com.hazelcast.stabilizer.common.AgentsFile;
import com.hazelcast.stabilizer.common.GitInfo;
import com.hazelcast.stabilizer.common.StabilizerProperties;
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static com.hazelcast.stabilizer.Utils.appendText;
import static com.hazelcast.stabilizer.Utils.fileAsText;
import static com.hazelcast.stabilizer.Utils.getVersion;
import static com.hazelcast.stabilizer.Utils.secondsToHuman;
import static java.lang.String.format;

//https://jclouds.apache.org/start/compute/ good read
//https://github.com/jclouds/jclouds-examples/blob/master/compute-basics/src/main/java/org/jclouds/examples/compute/basics/MainApp.java
//https://github.com/jclouds/jclouds-examples/blob/master/minecraft-compute/src/main/java/org/jclouds/examples/minecraft/NodeManager.java
public class Provisioner {
    private final static ILogger log = Logger.getLogger(Provisioner.class);

    public final StabilizerProperties props = new StabilizerProperties();

    private final static String STABILIZER_HOME = Utils.getStablizerHome().getAbsolutePath();
    private final static String CONF_DIR = STABILIZER_HOME + "/conf";

    private final File agentsFile = new File("agents.txt");
    //big number of threads, but they are used to offload ssh tasks. So there is no load on this machine..
    private final ExecutorService executor = Executors.newFixedThreadPool(10);

    private final List<AgentAddress> addresses = Collections.synchronizedList(new LinkedList<AgentAddress>());
    private Bash bash;
    private HazelcastJars hazelcastJars;
    private File initScript;

    public Provisioner() {
    }

    void init() throws Exception {
        if (!agentsFile.exists()) {
            agentsFile.createNewFile();
        }
        addresses.addAll(AgentsFile.load(agentsFile));
        bash = new Bash(props);
        hazelcastJars = new HazelcastJars(bash, props.getHazelcastVersionSpec());

        initScript = new File("init.sh");
        if (!initScript.exists()) {
            initScript = new File(CONF_DIR + "/init.sh");
        }
    }

    void installAgent(String ip) {
        bash.ssh(ip, format("mkdir -p hazelcast-stabilizer-%s", getVersion()));

        //first we remove the old lib files to prevent different versions of the same jar to bite us.
        bash.sshQuiet(ip, format("rm -fr hazelcast-stabilizer-%s/lib", getVersion()));
        bash.copyToAgentStabilizerDir(ip, STABILIZER_HOME + "/bin/", "bin");
        bash.copyToAgentStabilizerDir(ip, STABILIZER_HOME + "/conf/", "conf");
        bash.copyToAgentStabilizerDir(ip, STABILIZER_HOME + "/jdk-install/", "jdk-install");

        // we don't copy all jars to the agent since most of them are not needed.
        bash.copyToAgentStabilizerDir(ip, STABILIZER_HOME + "/lib/hazelcast*", "lib");
        bash.copyToAgentStabilizerDir(ip, STABILIZER_HOME + "/lib/jopt*", "lib");
        bash.copyToAgentStabilizerDir(ip, STABILIZER_HOME + "/lib/junit*", "lib");
        bash.copyToAgentStabilizerDir(ip, STABILIZER_HOME + "/lib/log4j*", "lib");
        bash.copyToAgentStabilizerDir(ip, STABILIZER_HOME + "/lib/stabilizer*", "lib");
        bash.copyToAgentStabilizerDir(ip, STABILIZER_HOME + "/lib/commons-lang3*", "lib");
        bash.copyToAgentStabilizerDir(ip, STABILIZER_HOME + "/lib/cache-api*", "lib");
        bash.copyToAgentStabilizerDir(ip, STABILIZER_HOME + "/lib/probes-*", "lib");
        bash.copyToAgentStabilizerDir(ip, STABILIZER_HOME + "/tests/", "tests");

        String script = loadInitScript();
        bash.ssh(ip, script);

        //we don't copy yourkit; it will be copied when the coordinator runs and sees that the profiler is enabled.
        //this is done to reduce the amount of data we need to upload.

        String versionSpec = props.getHazelcastVersionSpec();
        if (!versionSpec.equals("outofthebox")) {
            //todo: in the future we can improve this; we upload the hz jars, to delete them again.

            //remove the hazelcast jars, they will be copied from the 'hazelcastJarsDir'.
            bash.ssh(ip, format("rm -fr hazelcast-stabilizer-%s/lib/hazelcast-*.jar", getVersion()));

            if (!versionSpec.endsWith("bringmyown")) {
                //copy the actual hazelcast jars that are going to be used by the worker.
                bash.scpToRemote(ip, hazelcastJars.getAbsolutePath() + "/*.jar", format("hazelcast-stabilizer-%s/lib", getVersion()));
            }
        }
    }

    private String loadInitScript() {
        String script = Utils.fileAsText(initScript);

        script = script.replaceAll(Pattern.quote("${user}"), props.getUser());
        script = script.replaceAll(Pattern.quote("${version}"), getVersion());

        return script;
    }

    public void startAgents() {
        echoImportant("Starting %s Agents", addresses.size());

        for (AgentAddress address : addresses) {
            echo("Killing Agent %s", address.publicAddress);
            bash.ssh(address.publicAddress, "killall -9 java || true");
        }

        for (AgentAddress address : addresses) {
            echo("Starting Agent %s", address.publicAddress);

            bash.ssh(address.publicAddress, format(
                    "nohup hazelcast-stabilizer-%s/bin/agent --cloudProvider %s --cloudIdentity %s --cloudCredential %s> agent.out 2> agent.err < /dev/null &",
                    getVersion(), props.get("CLOUD_PROVIDER"), props.get("CLOUD_IDENTITY"), props.get("CLOUD_CREDENTIAL")));
        }

        echoImportant("Successfully started %s Agents", addresses.size());
    }

    void startAgent(String ip) {
        bash.ssh(ip, "killall -9 java || true");
        bash.ssh(ip, format("nohup hazelcast-stabilizer-%s/bin/agent -cloudProvider %s --cloudIdentity %s --cloudCredential %s> agent.out 2> agent.err < /dev/null &",
                getVersion(), props.get("CLOUD_PROVIDER"), props.get("CLOUD_IDENTITY"), props.get("CLOUD_CREDENTIAL")));
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
        echoImportant("Restarting %s Agents", addresses.size());

        hazelcastJars.prepare();
        for (AgentAddress address : addresses) {
            echo("Installing agent: " + address.publicAddress);
            installAgent(address.publicAddress);
        }

        echoImportant("Restarting %s Agents", addresses.size());
    }

    public void scale(int size) throws Exception {
        int delta = size - addresses.size();
        if (delta == 0) {
            echo("Current number of machines: " + addresses.size());
            echo("Desired number of machines: " + (addresses.size() + delta));
            echo("Ignoring spawn machines, desired number of machines already exists.");
        } else if (delta < 0) {
            terminate(-delta);
        } else {
            scaleUp(delta);
        }
    }

    private void scaleUp(int delta) throws Exception {
        echoImportant("Provisioning %s %s machines", delta, props.get("CLOUD_PROVIDER"));
        echo("Current number of machines: " + addresses.size());
        echo("Desired number of machines: " + (addresses.size() + delta));
        String groupName = props.get("GROUP_NAME", "stabilizer-agent");
        echo("GroupName: " + groupName);
        echo("Username: " + props.getUser());

        log.info("Using init script:"+initScript.getAbsolutePath());

        long startTimeMs = System.currentTimeMillis();

        String jdkFlavor = props.get("JDK_FLAVOR", "outofthebox");
        if ("outofthebox".equals(jdkFlavor)) {
            log.info("JDK spec: outofthebox");
        } else {
            String jdkVersion = props.get("JDK_VERSION", "7");
            log.info(format("JDK spec: %s %s", jdkFlavor, jdkVersion));
        }

        hazelcastJars.prepare();

        ComputeService compute = new ComputeServiceBuilder(props).build();

        echo("Created compute");

        Template template = new TemplateBuilder(compute, props).build();

        echo("Creating machines (can take a few minutes)");

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
                Future f = executor.submit(new InstallNodeTask(publicIpAddress));
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

        echo("Pausing for machine warmup... (10 sec)");
        Thread.sleep(10000);

        long durationMs = System.currentTimeMillis() - startTimeMs;
        echo("Duration: " + secondsToHuman(TimeUnit.MILLISECONDS.toSeconds(durationMs)));
        echoImportant(format("Successfully provisioned %s %s machines",
                delta, props.get("CLOUD_PROVIDER")));
    }

    public void listAgents() {
        echo("Running Agents (from agents.txt):");
        String agents = fileAsText(agentsFile);
        echo("    " + agents);
    }

    private class InstallNodeTask implements Runnable {
        private final String ip;

        private InstallNodeTask(String ip) {
            this.ip = ip;
        }

        @Override
        public void run() {
            //install java if needed
            if (!"outofthebox".equals(props.get("JDK_FLAVOR"))) {
                bash.scpToRemote(ip, getJavaSupportScript(), "jdk-support.sh");
                bash.scpToRemote(ip, getJavaInstallScript(), "install-java.sh");
                bash.ssh(ip, "bash install-java.sh");
                echo("    " + ip + " JAVA INSTALLED");
            }

            installAgent(ip);
            echo("    " + ip + " STABILIZER AGENT INSTALLED");

            startAgent(ip);
            echo("    " + ip + " STABILIZER AGENT STARTED");
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

    private File getJavaInstallScript() {
        String flavor = props.get("JDK_FLAVOR");
        String version = props.get("JDK_VERSION");

        String script = "jdk-" + flavor + "-" + version + "-64.sh";
        File scriptDir = new File(STABILIZER_HOME, "jdk-install");
        return new File(scriptDir, script);
    }

    private File getJavaSupportScript() {
        File scriptDir = new File(STABILIZER_HOME, "jdk-install");
        return new File(scriptDir, "jdk-support.sh");
    }

    public void download(String dir) {
        echoImportant("Download artifacts of %s machines", addresses.size());

        bash.execute("mkdir -p "+dir);

        for (AgentAddress address : addresses) {
            echo("Downloading from %s", address.publicAddress);

            String syncCommand = format("rsync --copy-links  -av -e \"ssh %s\" %s@%s:hazelcast-stabilizer-%s/workers/* ./"+dir,
                    props.get("SSH_OPTIONS", ""), props.getUser(), address.publicAddress, getVersion());

            bash.executeQuiet(syncCommand);
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

        echoImportant(format("Terminating %s %s machines (can take some time)", count, props.get("CLOUD_PROVIDER")));
        echo("Current number of machines: " + addresses.size());
        echo("Desired number of machines: " + (addresses.size() - count));

        long startMs = System.currentTimeMillis();

        ComputeService compute = new ComputeServiceBuilder(props).build();

        for (int batchSize : calcBatches(count)) {
            final Map<String, AgentAddress> terminateMap = new HashMap<String, AgentAddress>();
            for (AgentAddress address : addresses.subList(0, batchSize)) {
                terminateMap.put(address.publicAddress, address);
            }
            destroyNodes(compute, terminateMap);
        }

        log.info("Updating " + agentsFile.getAbsolutePath());

        AgentsFile.save(agentsFile, addresses);

        long durationSeconds = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - startMs);
        echo("Duration: " + secondsToHuman(durationSeconds));
        echoImportant("Finished terminating %s %s machines, %s machines remaining.",
                count, props.get("CLOUD_PROVIDER"), addresses.size());
    }

    private void destroyNodes(ComputeService compute, final Map<String, AgentAddress> terminateMap) {
        compute.destroyNodesMatching(
                new Predicate<NodeMetadata>() {
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
                }
        );
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
        log.info("Hazelcast Stabilizer Provisioner");
        log.info(format("Version: %s, Commit: %s, Build Time: %s",
                getVersion(), GitInfo.getCommitIdAbbrev(), GitInfo.getBuildTime()));
        log.info(format("STABILIZER_HOME: %s", STABILIZER_HOME));

        try {
            Provisioner provisioner = new Provisioner();
            ProvisionerCli cli = new ProvisionerCli(provisioner);
            cli.run(args);
            System.exit(0);
        } catch (Throwable e) {
            log.severe(e);
            System.exit(1);
        }
    }
}
