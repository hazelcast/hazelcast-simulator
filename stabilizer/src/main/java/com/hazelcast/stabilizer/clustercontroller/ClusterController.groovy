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
import org.jclouds.scriptbuilder.domain.Statements
import org.jclouds.sshj.config.SshjSshClientModule

import static com.hazelcast.stabilizer.Utils.*
import static java.lang.String.format
import static java.util.Arrays.asList

public class ClusterController {
    private final static ILogger log = Logger.getLogger(ClusterController.class.getName());

    def AGENT_PORT = '8701'
    def config
    def STABILIZER_HOME = Utils.getStablizerHome().getAbsolutePath()
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
        echo "=============================================================="
        echo "Installing Agent on ${ip}"
        echo "=============================================================="

        echo "Installing missing Java"
        //install java under Ubuntu.
        sshQuiet ip, "sudo apt-get update || true"
        sshQuiet ip, "sudo apt-get install -y openjdk-7-jdk || true"

        echo "Copying stabilizer files"
        //first we remove the old lib files to prevent different versions of the same jar to bite us.
        sshQuiet ip, "rm -fr hazelcast-stabilizer-${getVersion()}/lib"
        //then we copy the stabilizer directory
        scpToRemote ip, STABILIZER_HOME, ""

        echo "=============================================================="
        echo "Successfully installed Agent on ${ip}"
        echo "=============================================================="
    }

    void startAgents() {
        echo "=============================================================="
        echo "Starting ${privateIps.size()} Agents"
        echo "=============================================================="

        privateIps.each { String ip ->
            echo "Killing Agent $ip"
            ssh ip, "killall -9 java || true"
        }

        privateIps.each { String ip ->
            echo "Starting Agent $ip"
            ssh ip, "nohup hazelcast-stabilizer-${getVersion()}/bin/agent > agent.out 2> agent.err < /dev/null &"
        }

        echo "=============================================================="
        echo "Successfully started ${privateIps.size()} Agents"
        echo "=============================================================="
    }

    void killAgents() {
        echo "=============================================================="
        echo "Killing ${privateIps.size()} Agents"
        echo "=============================================================="

        privateIps.each { String ip ->
            echo "Killing Agent $ip"
            ssh ip, "killall -9 java || true"
        }

        echo "=============================================================="
        echo "Successfully killed ${privateIps.size()} Agents"
        echo "=============================================================="
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
        int delta = calcSize(sizeType) - privateIps.size()
        if (delta == 0) {
            echo "Ignoring spawn machines, desired number of machines already exists"
        } else if (delta < 0) {
            terminate(-delta)
        } else {
            scaleUp(delta)
        }
    }

    private void scaleUp(int delta) {
        echo "=============================================================="
        echo "Starting ${delta} ${config.CLOUD_PROVIDER} machines"
        echo "=============================================================="

        ComputeService compute = getComputeService()

        Template template = compute.templateBuilder()
                .from(TemplateBuilderSpec.parse(config.MACHINE_SPEC))
                .build();

        template.getOptions()
                .inboundPorts(22, AgentRemoteService.PORT, WorkerJvmManager.PORT)
                .authorizePublicKey(fileAsText(config.PUBLIC_KEY))
                .blockUntilRunning(true)
                .securityGroups("open")

        Set<NodeMetadata> nodes = compute.createNodesInGroup("stabilizer-agent", delta, template)
        println "Created machines, waiting for startup"

        for (NodeMetadata m : nodes) {
            String ip = m.privateAddresses.iterator().next()
            echo("\t" + ip + " LAUNCHED");
            appendText(ip + "\n", agentsFile)
        }

        LoginCredentials login = LoginCredentials.builder()
                .privateKey(fileAsText(config.IDENTITY_FILE))
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

        echo "=============================================================="
        echo "Successfully started ${delta} ${config.CLOUD_PROVIDER} machines "
        echo "=============================================================="

        installAgents()
        startAgents()
    }

    private ComputeService getComputeService() {
        ComputeService compute = ContextBuilder.newBuilder(config.CLOUD_PROVIDER)
                .credentials(config.CLOUD_IDENTITY, config.CLOUD_CREDENTIAL)
                .modules(asList(new Log4JLoggingModule(), new SshjSshClientModule()))
                .buildView(ComputeServiceContext.class)
                .getComputeService();
        compute
    }

    def downloadArtifacts() {
        echo "=============================================================="
        echo "Download artifacts of ${privateIps.size()} machines"
        echo "=============================================================="

        bash "mkdir -p workers"

        privateIps.each { String ip ->
            echo "Downoading from $ip"
            bash """rsync -av -e "ssh ${config.SSH_OPTIONS}" \
                ${config.USER}@$ip:hazelcast-stabilizer-${getVersion()}/workers/ \
                workers"""
        }

        echo "=============================================================="
        echo "Finished Downloading Artifacts of ${privateIps.size()} machines"
        echo "=============================================================="
    }

    void terminate(count = Integer.MAX_VALUE) {
        if (count > privateIps.size()) {
            count = privateIps.size()
        }

        log.info("==============================================================");
        log.info(format("Terminating %s %s machines", count, config.CLOUD_PROVIDER));
        log.info("==============================================================");

        final List<String> terminateList = privateIps.subList(0, count);

        ComputeService computeService = getComputeService();
        computeService.destroyNodesMatching(
                new Predicate<NodeMetadata>() {
                    @Override
                    boolean apply(NodeMetadata nodeMetadata) {
                        for (String ip : nodeMetadata.privateAddresses) {
                            if (terminateList.remove(ip)) {
                                echo(format("\t%s Terminated", ip))
                                privateIps.remove(ip)
                                return true;
                            }
                        }
                        return false;
                    }
                }
        )

        agentsFile.write("")
        privateIps.each { String ip ->
            agentsFile.text += "$ip\n"
        }

        log.info("==============================================================");
        log.info("Finished terminating $count ${config.CLOUD_PROVIDER} machines");
        log.info("==============================================================");
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
        String command = "scp -r ${config.SSH_OPTIONS} $src ${config.USER}@$ip:$target"
        bash(command)
    }

    void ssh(String ip, String command) {
        String sshCommand = "ssh ${config.SSH_OPTIONS} -q ${config.USER}@$ip \"$command\""
        bash sshCommand
    }

    void sshQuiet(String ip, String command) {
        String sshCommand = "ssh ${config.SSH_OPTIONS} -q ${config.USER}@$ip \"$command\" || true"
        bash sshCommand
    }

    void echo(Object s) {
        log.info(s)
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
