package com.hazelcast.stabilizer.provisioner;

import com.google.common.base.Predicate;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.Utils;
import com.hazelcast.stabilizer.agent.AgentRemoteService;
import com.hazelcast.stabilizer.agent.workerjvm.WorkerJvmManager;
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
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.stabilizer.Utils.appendText;
import static com.hazelcast.stabilizer.Utils.fileAsLines;
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
    private final File agentsFile = new File("agents.txt");
    //big number of threads, but they are used to offload ssh tasks. So there is no load on this machine..
    private final ExecutorService executor = Executors.newFixedThreadPool(10);

    private final List<String> privateIps = Collections.synchronizedList(new LinkedList<String>());

    public Provisioner() throws Exception {
        log.info("Hazelcast Stabilizer Provisioner");
        log.info(format("Version: %s", getVersion()));
        log.info(format("STABILIZER_HOME: %s", STABILIZER_HOME));

        if (!agentsFile.exists()) {
            agentsFile.createNewFile();
        }

        for (String line : fileAsLines(agentsFile)) {
            if (line.length() > 0) {
                privateIps.add(line);
            }
        }
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
//        echoImportant "Installing Agent on ${ip}"

//        echo "Copying stabilizer files"
        //first we remove the old lib files to prevent different versions of the same jar to bite us.
        sshQuiet(ip, format("rm -fr hazelcast-stabilizer-%s/lib", VERSION));
        //then we copy the stabilizer directory
        scpToRemote(ip, STABILIZER_HOME, "");

//        echoImportant("Successfully installed Agent on ${ip}");
    }


    public void startAgents() {
        echoImportant("Starting %s Agents", privateIps.size());

        for (String ip : privateIps) {
            echo("Killing Agent %s", ip);
            ssh(ip, "killall -9 java || true");
        }

        for (String ip : privateIps) {
            echo("Starting Agent %s", ip);
            ssh(ip, format("nohup hazelcast-stabilizer-%s/bin/agent > agent.out 2> agent.err < /dev/null &", VERSION));
        }

        echoImportant("Successfully started %s Agents", privateIps.size());
    }

    void startAgent(String ip) {
//        echoImportant("Starting ${privateIps.size()} Agents");

//        for (String ip : privateIps) {
//            echo "Killing Agent $ip"
        ssh(ip, "killall -9 java || true");
//        }

        //       for (String ip : privateIps) {
        //           echo "Starting Agent $ip"
        ssh(ip, format("nohup hazelcast-stabilizer-%s/bin/agent > agent.out 2> agent.err < /dev/null &", getVersion()));
        //       }

        //       echoImportant("Successfully started ${privateIps.size()} Agents");
    }

    void killAgents() {
        echoImportant("Killing %s Agents", privateIps.size());

        for (String ip : privateIps) {
            echo("Killing Agent, %s", ip);
            ssh(ip, "killall -9 java || true");
        }

        echoImportant("Successfully killed %s Agents", privateIps.size());
    }

    public void installAgents() {
        for (String ip : privateIps) {
            installAgent(ip);
        }
    }

    public void scale(int size) throws Exception {
        int delta = size - privateIps.size();
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
                String ip = node.getPrivateAddresses().iterator().next();
                echo("\t" + ip + " LAUNCHED");
                appendText(ip + "\n", agentsFile);
                privateIps.add(ip);
            }

            for (NodeMetadata node : nodes) {
                Future f = executor.submit(new InstallNodeTask(node, compute));
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
        private final NodeMetadata node;
        private final ComputeService compute;
        private final String ip;

        InstallNodeTask(NodeMetadata node, ComputeService compute) {
            this.node = node;
            this.compute = compute;
            this.ip = node.getPrivateAddresses().iterator().next();
        }

        @Override
        public void run() {
            //install java if needed
            if (!"outofthebox".equals(getProperty("JDK_FLAVOR"))) {
                ssh(ip, "touch install-java.sh");
                ssh(ip, "chmod +x install-java.sh");
                scpToRemote(ip, getJavaInstallScript().getAbsolutePath(), "install-java.sh");
                ssh(ip, "sudo bash install-java.sh");
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

        String script = "jdk-" + flavor + "-" + version + ".sh";
        return new File(CONF_DIR, script);
    }

    public void download() {
        echoImportant("Download artifacts of %s machines", privateIps.size());

        bash("mkdir -p workers");

        for (String ip : privateIps) {
            echo("Downloading from %s", ip);

            String syncCommand = format("rsync  -av -e \"ssh %s\" %s@%s:hazelcast-stabilizer-%s/workers .",
                    getProperty("SSH_OPTIONS"), getProperty("USER"), ip, getVersion());

            bash(syncCommand);
        }

        echoImportant("Finished Downloading Artifacts of %s machines", privateIps.size());
    }

    public void clean() {
        echoImportant("Cleaning worker homes of %s machines", privateIps.size());

        for (String ip : privateIps) {
            echo("Cleaning %s", ip);
            ssh(ip, format("rm -abc -fr hazelcast-stabilizer-%s/workers/*", getVersion()));
        }

        echoImportant("Finished cleaning worker homes of %s machines", privateIps.size());
    }

    public void terminate() {
        terminate(Integer.MAX_VALUE);
    }

    public void terminate(int count) {
        if (count > privateIps.size()) {
            count = privateIps.size();
        }

        //System.out.println("current number of machines is: " + privateIps.size());
        echoImportant(format("Terminating %s %s machines (can take some time)", count, getProperty("CLOUD_PROVIDER")));

        long startMs = System.currentTimeMillis();

        for (int batch : calcBatches(count)) {

            final List<String> terminateList = privateIps.subList(0, batch);

            ComputeService computeService = getComputeService();
            computeService.destroyNodesMatching(
                    new Predicate<NodeMetadata>() {
                        @Override
                        public boolean apply(NodeMetadata nodeMetadata) {
                            for (String ip : nodeMetadata.getPrivateAddresses()) {
                                if (terminateList.remove(ip)) {
                                    echo(format("\t%s Terminating", ip));
                                    privateIps.remove(ip);
                                    return true;
                                }
                            }
                            return false;
                        }
                    }
            );
        }

        log.info("Updating " + agentsFile.getAbsolutePath());

        writeAgentsFile();

        long durationSeconds = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - startMs);
        echo("Duration: " + secondsToHuman(durationSeconds));
        echoImportant("Finished terminating %s %s machines, %s machines remaining.",
                count, CLOUD_PROVIDER, privateIps.size());
    }

    private void writeAgentsFile() {
        String text = "";
        for (String ip : privateIps) {
            text += ip + "\n";
        }
        Utils.writeText(text, agentsFile);
    }

    private void bash(String command) {
        StringBuffer sout = new StringBuffer();

        try {
            // create a process for the shell
            ProcessBuilder pb = new ProcessBuilder("bash", "-c", command);
            pb = pb.redirectErrorStream(true);

            Process shell = pb.start();
            new StreamGobbler(shell.getInputStream(), sout).start();

            // wait for the shell to finish and get the return code
            int shellExitStatus = shell.waitFor();
            if (shellExitStatus != 0) {
                echo("Failed to execute [%s]", command);
                log.severe(sout.toString());
                System.exit(1);
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
                    provisioner.installAgents();
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
        } catch (OptionException e) {
            Utils.exitWithError(e.getMessage() + ". Use --help to get overview of the help options.");
        } catch (Throwable e) {
            log.severe(e);
            System.exit(1);
        }
    }
}
