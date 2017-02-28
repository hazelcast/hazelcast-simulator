/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.simulator.coordinator;

import com.hazelcast.simulator.common.SimulatorProperties;
import com.hazelcast.simulator.protocol.connector.CoordinatorRemoteConnector;
import com.hazelcast.simulator.protocol.core.AddressLevel;
import com.hazelcast.simulator.protocol.core.Response;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.operation.RcDownloadOperation;
import com.hazelcast.simulator.protocol.operation.RcInstallOperation;
import com.hazelcast.simulator.protocol.operation.RcPrintLayoutOperation;
import com.hazelcast.simulator.protocol.operation.RcStopCoordinatorOperation;
import com.hazelcast.simulator.protocol.operation.RcTestRunOperation;
import com.hazelcast.simulator.protocol.operation.RcTestStatusOperation;
import com.hazelcast.simulator.protocol.operation.RcTestStopOperation;
import com.hazelcast.simulator.protocol.operation.RcWorkerKillOperation;
import com.hazelcast.simulator.protocol.operation.RcWorkerScriptOperation;
import com.hazelcast.simulator.protocol.operation.RcWorkerStartOperation;
import com.hazelcast.simulator.protocol.operation.SimulatorOperation;
import com.hazelcast.simulator.protocol.registry.TargetType;
import com.hazelcast.simulator.protocol.registry.WorkerQuery;
import com.hazelcast.simulator.utils.CommandLineExitException;
import com.hazelcast.simulator.utils.FileUtils;
import com.hazelcast.simulator.utils.TagUtils;
import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.NonOptionArgumentSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.log4j.Logger;

import java.io.Closeable;
import java.io.File;
import java.util.LinkedList;
import java.util.List;

import static com.hazelcast.simulator.coordinator.CoordinatorCli.DEFAULT_DURATION_SECONDS;
import static com.hazelcast.simulator.coordinator.CoordinatorCli.getDurationSeconds;
import static com.hazelcast.simulator.utils.CliUtils.initOptionsOnlyWithHelp;
import static com.hazelcast.simulator.utils.CliUtils.initOptionsWithHelp;
import static com.hazelcast.simulator.utils.CommonUtils.closeQuietly;
import static com.hazelcast.simulator.utils.CommonUtils.exitWithError;
import static com.hazelcast.simulator.utils.FileUtils.fileAsText;
import static java.lang.String.format;
import static java.lang.System.arraycopy;

/**
 * CLI to access Simulator Coordinator remotely.
 */
public final class CoordinatorRemoteCli implements Closeable {

    private static final Logger LOGGER = Logger.getLogger(CoordinatorRemoteCli.class);

    private final String[] args;
    private final int coordinatorPort;

    private CoordinatorRemoteConnector connector;

    private CoordinatorRemoteCli(String[] args) {
        SimulatorProperties simulatorProperties = new SimulatorProperties();
        File file = new File(FileUtils.getUserDir(), "simulator.properties");
        if (file.exists()) {
            simulatorProperties.init(file);
        }

        this.args = args;
        this.coordinatorPort = simulatorProperties.getCoordinatorPort();
        if (coordinatorPort == 0) {
            throw new CommandLineExitException("Coordinator port is disabled!");
        }
    }

    @SuppressWarnings("checkstyle:cyclomaticcomplexity")
    public void run() {
        if (args.length == 0) {
            printHelpAndExit();
        }

        String cmd = args[0];
        String[] subArgs = removeFirst(args);

        connector = new CoordinatorRemoteConnector("localhost", coordinatorPort);
        connector.start();
        if ("download".equals(cmd)) {
            new DownloadCli().run(subArgs);
        } else if ("install".equals(cmd)) {
            new InstallCli().run(subArgs);
        } else if ("print-layout".equals(cmd)) {
            new PrintClusterLayoutCli().run(subArgs);
        } else if ("stop".equals(cmd)) {
            new ExitCli().run(subArgs);
        } else if ("test-run".equals(cmd)) {
            new TestRunCli().run(subArgs);
        } else if ("test-start".equals(cmd)) {
            new TestStartCli().run(subArgs);
        } else if ("test-status".equals(cmd)) {
            new TestStatusCli().run(subArgs);
        } else if ("test-stop".equals(cmd)) {
            new TestStopCli().run(subArgs);
        } else if ("worker-kill".equals(cmd)) {
            new WorkerKill().run(subArgs);
        } else if ("worker-script".equals(cmd)) {
            new WorkerScriptCli().run(subArgs);
        } else if ("worker-start".equals(cmd)) {
            new WorkerStartCli().run(subArgs);
        } else {
            printHelpAndExit();
        }
    }

    private static void printHelpAndExit() {
        System.out.println(
                "Command         Description                                                                 \n"
                        + "--------------------------                                                                  \n"
                        + "download        Downloads all artifacts from the workers                                    \n"
                        + "install         Installs vendor software on the remote machines                             \n"
                        + "print-layout    Prints the cluster-layout                                                   \n"
                        + "test-run        Runs a test and wait for completion                                         \n"
                        + "test-start      Starts a test asynchronously                                                \n"
                        + "test-stop       Stops a test                                                                \n"
                        + "test-status     Checks the status of a test                                                 \n"
                        + "stop            Stops the Coordinator remote session                                        \n"
                        + "worker-kill     Kills one or more workers                                                   \n"
                        + "worker-script   Executes a script on workers                                                \n"
                        + "worker-start    Starts workers                                                              ");

        System.exit(1);
    }

    @Override
    public void close() {
        closeQuietly(connector);
    }

    public static void main(String[] args) throws Exception {
        CoordinatorRemoteCli cli = null;
        try {
            cli = new CoordinatorRemoteCli(args);
            cli.run();
        } catch (Exception e) {
            System.err.print(e.getMessage());
            exitWithError(LOGGER, e.getMessage(), e);
        } finally {
            closeQuietly(cli);
        }
    }

    private static String[] removeFirst(String[] args) {
        String[] result = new String[args.length - 1];
        arraycopy(args, 1, result, 0, result.length);
        return result;
    }

    private abstract class AbstractCli {

        protected final OptionParser parser = new OptionParser();

        protected OptionSet options;

        protected abstract SimulatorOperation newOperation();

        protected abstract OptionSet newOptions(String[] args);

        protected void run(String[] args) {
            this.options = newOptions(args);

            Response response = connector.write(newOperation());

            Response.Part errorPart = response.getFirstErrorPart();

            if (errorPart == null) {
                Response.Part part = response.getFirstPart();
                System.out.println(part.getPayload() == null ? "success" : part.getPayload());
            } else {
                if (errorPart.getPayload() != null) {
                    System.err.println(errorPart.getPayload());
                } else {
                    System.err.println("errorType: " + errorPart.getType());
                }
                throw new CommandLineExitException(
                        format("Could not process command: %s message [%s]", errorPart.getType(), errorPart.getPayload()));
            }
        }
    }

    private class InstallCli extends AbstractCli {

        private final String help
                = "The 'install' command installs Hazelcast on the agents. By default the coordinator will upload to\n"
                + "the agents what has been configured on the simulator.properties. But in case of testing multiple\n"
                + "versions the other versions need to be installed. If a worker is started without the right software\n"
                + "the worker will fail to start\n"
                + "\n"
                + "In case of the maven version spec, the local repository is always preferred and no update checking\n"
                + "is done in case of SNAPSHOT version. This is very useful for testing custom SNAPSHOT branches, but\n"
                + "can be a problem if the local SNAPSHOT is not updated and the user expects to use the latest\n"
                + "SNAPSHOT.\n"
                + "\n"
                + "Examples\n"
                + "# installs Hazelcast 3.6 from local or remote repo.\n"
                + "coordinator-remote install maven=3.6\n\n"
                + "# installs Hazelcast 3.8-SNAPSHOT from local or remote repo.\n"
                + "coordinator-remote install maven=3.8-SNAPSHOT\n\n"
                + "# installs Hazelcast using some git commit hash.\n"
                + "coordinator-remote install git=<somehash>\n";

        private final NonOptionArgumentSpec<String> argumentSpec = parser
                .nonOptions("version specification").ofType(String.class);

        @Override
        protected OptionSet newOptions(String[] args) {
            return initOptionsWithHelp(parser, help, args);
        }

        @Override
        protected SimulatorOperation newOperation() {
            List<String> nonOptionArguments = options.valuesOf(argumentSpec);
            if (nonOptionArguments.size() != 1) {
                throw new CommandLineExitException("Too many arguments");
            }

            String versionSpec = nonOptionArguments.get(0);
            LOGGER.info("Installing [" + versionSpec + "]");
            return new RcInstallOperation(versionSpec);
        }
    }

    private class TestStatusCli extends AbstractCli {

        private final String help =
                "Returns the status of a test\n"
                        + "Possible values:\n"
                        + "\tcompleted\n"
                        + "\tfailed\n"
                        + "\tsetup\n"
                        + "\tlocal prepare\n"
                        + "\tglobal prepare\n"
                        + "\twarmup\n"
                        + "\tlocal after warmup\n"
                        + "\tglobal after warmup\n"
                        + "\trun\n"
                        + "\tglobal verify\n"
                        + "\tlocal verify\n"
                        + "\tglobal tear down\n"
                        + "\tlocal tear down\n"
                        + "\n"
                        + "Examples\n"
                        + "# Checks the status of some test.\n"
                        + "coordinator-remote test-status C_A*_W*_T1\n";

        private final NonOptionArgumentSpec<String> argumentSpec = parser
                .nonOptions("test address").ofType(String.class);

        @Override
        protected OptionSet newOptions(String[] args) {
            return initOptionsWithHelp(parser, help, args);
        }

        @Override
        protected SimulatorOperation newOperation() {
            List<String> nonOptionArguments = options.valuesOf(argumentSpec);
            if (nonOptionArguments.size() != 1) {
                throw new CommandLineExitException("Too many arguments");
            }

            String testId = nonOptionArguments.get(0);
            return new RcTestStatusOperation(testId);
        }
    }

    private class TestStopCli extends AbstractCli {

        private final String help =
                "Ask a test to stop its warmup or running phase. It is especially useful for tests that run without a\n"
                        + "duration.\n"
                        + "This commands waits for the test to complete and returns the result status of the test\n"
                        + " ('completed' for success)\n"
                        + "\n"
                        + "Examples\n"
                        + "# Stops a test.\n"
                        + "coordinator-remote test-stop C_A*_W*_T1\n";

        private final NonOptionArgumentSpec<String> argumentSpec = parser
                .nonOptions("test address").ofType(String.class);

        @Override
        protected OptionSet newOptions(String[] args) {
            return initOptionsWithHelp(parser, help, args);
        }

        @Override
        protected SimulatorOperation newOperation() {
            List<String> nonOptionArguments = options.valuesOf(argumentSpec);
            if (nonOptionArguments.size() != 1) {
                throw new CommandLineExitException("Too many arguments");
            }

            String testId = nonOptionArguments.get(0);
            return new RcTestStopOperation(testId);
        }
    }

    private class DownloadCli extends AbstractCli {

        private final String help = ""
                + "The download command downloads all artifacts from the workers.\n";

        @Override
        protected OptionSet newOptions(String[] args) {
            return initOptionsOnlyWithHelp(parser, help, args);
        }

        @Override
        protected SimulatorOperation newOperation() {
            return new RcDownloadOperation();
        }
    }

    private abstract class WorkerQueryableCli extends AbstractCli {

        final OptionSpec<String> versionSpecSpec = parser.accepts("versionSpec",
                "The versionSpec of the worker to select e.g  maven=3.7 or git=master")
                .withRequiredArg().ofType(String.class);

        final ArgumentAcceptingOptionSpec<String> workerTypeSpec = parser.accepts("workerType",
                "The type of machine to select, e.g. member, litemember, javaclient (native clients will be added soon) etc")
                .withRequiredArg().ofType(String.class);

        final ArgumentAcceptingOptionSpec<Integer> maxCountSpec = parser.accepts("maxCount",
                "The maximum number of workers to select. It can safely be called with a maxCount larger than "
                        + "the actual number of workers.")
                .withRequiredArg().ofType(Integer.class);

        final OptionSpec<String> agentsSpec = parser.accepts("agents",
                "Comma separated list of agent simulator addresses containing the workers to select")
                .withRequiredArg().ofType(String.class);

        final OptionSpec<String> workersSpec = parser.accepts("workers",
                "Comma separated list of simulator addresses of the workers to select. If this option is set, all other search "
                        + " constraints are ignored.")
                .withRequiredArg().ofType(String.class);

        final OptionSpec<String> workerTagsSpec = parser.accepts("workerTags",
                "worker tags to look for.")
                .withRequiredArg().ofType(String.class);

        final OptionSpec randomSpec = parser.accepts("random",
                "If workers should be picked randomly or predictably");

        WorkerQuery newQuery() {
            WorkerQuery query = new WorkerQuery().setRandom(options.has(randomSpec));

            List<String> workerAddresses = loadAddresses(options, workersSpec, AddressLevel.WORKER);
            if (workerAddresses == null) {
                List<String> agentAddresses = loadAddresses(options, agentsSpec, AddressLevel.AGENT);

                Integer maxCount = options.valueOf(maxCountSpec);
                if (maxCount != null) {
                    if (maxCount <= 0) {
                        throw new CommandLineExitException("--maxCount can't be smaller than 1");
                    }
                }

                return query.setAgentAddresses(agentAddresses)
                        .setWorkerType(options.valueOf(workerTypeSpec))
                        .setVersionSpec(options.valueOf(versionSpecSpec))
                        .setWorkerTags(TagUtils.loadTags(options, workerTagsSpec))
                        .setMaxCount(maxCount);
            } else {
                return query.setWorkerAddresses(workerAddresses);
            }
        }
    }

    private class WorkerScriptCli extends WorkerQueryableCli {

        private final String help
                = "The 'worker-script' commands executes a Bash-script or Javascript on workers\n"
                + "\n"
                + "Various filter options are available like --versionSpec, --workerType, --agent, --worker\n"
                + "\n"
                + "By default the script is executed on all members selected, but can be controlled using the\n"
                + "--maxCount setting.\n"
                + "\n"
                + "By default the selection of members is deterministic, however using the --random setting\n"
                + "one can enable shuffling of members."
                + "\n"
                + "The bash and javascript commands are very useful for High Availability testing by e.g\n"
                + "causing split brains, consuming most memory, consuming CPU cycles, and poking in Hazelcast\n"
                + "internals\n"
                + "\n"
                + "By default this command waits till the commands have been processed by all workers, but in\n"
                + "some cases this is undesirable e.g. when the task takes a lot of time. If waiting for\n"
                + "completion is undesirable, use the --fireAndForget option\n"
                + "\n"
                + "Examples\n"
                + "# takes a threadump on all workers\n"
                + "coordinator-remote worker-script  --command 'bash:jstack $PID''\n\n"
                + "# takes a threadump on at most 2 workers\n"
                + "coordinator-remote worker-script  --maxCount 2 --command 'bash:jstack $PID''\n\n"
                + "# takes a threadump on all member\n"
                + "coordinator-remote worker-script  --workerType member --command 'bash:jstack $PID''\n\n"
                + "# takes a threadump on all workers with a specific version\n"
                + "coordinator-remote worker-script  --versionSpec maven=3.7 --command 'bash:jstack $PID''\n\n"
                + "# takes a threaddump on all member on agent C_A1\n"
                + "coordinator-remote worker-script --workerType member --agents C_A1 --command 'bash:jstack $PID'\n\n"
                + "# takes a threaddump on C_A1_W1\n"
                + "coordinator-remote worker-script --workers C_A1_W1 --command 'bash:jstack $PID'\n\n"
                + "# executes a javascript on all workers\n"
                + "coordinator-remote worker-script --command 'js:java.lang.System.out.println(\"hello\")'";

        private final OptionSpec fireAndForget = parser.accepts("fireAndForget",
                "If the command is a fire and forget and no waiting for a response.");

        @Override
        protected OptionSet newOptions(String[] args) {
            workerTypeSpec.defaultsTo("member");
            return initOptionsWithHelp(parser, help, args);
        }

        @Override
        protected SimulatorOperation newOperation() {
            List<?> nonOptionArguments = options.nonOptionArguments();
            if (nonOptionArguments.size() != 1) {
                throw new CommandLineExitException("Only 1 argument allowed. Use single quotes, e.g. 'bash:jstack $PID'");
            }

            WorkerQuery workerQuery = newQuery();

            String cmd = (String) nonOptionArguments.get(0);
            LOGGER.info("Executing [" + cmd + "]");
            return new RcWorkerScriptOperation(cmd, workerQuery, options.has(fireAndForget));
        }
    }

    private class WorkerKill extends WorkerQueryableCli {

        private final String help
                = "The 'worker-kill' command kills one or more workers. The killing can be done based using an exact\n"
                + "worker address or using various filters like versionSpec, etc.\n"
                + "\n"
                + "By default the selection of members is deterministic, however using the --randomSpec setting\n"
                + "one can enable shuffling of members."
                + "\n"
                + "By default the worker is killed using a System.exit call, however one can also use a bash script\n"
                + "or a javascript which is executed inside the JMV\n"
                + "\n"
                + "If a script is used, the big difference between the 'worker-script' and the 'worker-kill' command\n"
                + "is when the 'worker-script' kills the JVM, it will lead to a failure. With the 'worker-kill' the\n"
                + "failure is expected and the call will wait till the worker has actually died.\n"
                + "\n"
                + "By default there is no maximum of the items to kill, so if no criteria are given, all workers are\n"
                + "killed!\n"
                + "\n"
                + "Examples\n"
                + "# kills one member using System.exit(0)\n"
                + "coordinator-remote --maxCount 1 worker-kill\n\n"
                + "# kill 2 java clients\n"
                + "coordinator-remote worker-kill --maxCount 2 --workerType javaclient\n\n"
                + "# kills 3 litemembers using version spec git=master clients\n"
                + "coordinator-remote worker-kill --maxCount 3 --workerType litemember --versionSpec git=master\n\n"
                + "# kills all workers on agent C_A1\n"
                + "coordinator-remote worker-kill --agent C_A1 \n\n"
                + "# kills worker C_A1_W1\n"
                + "coordinator-remote worker-kill --worker C_A1_W1 \n\n"
                + "# kill one member using OOME\n"
                + "coordinator-remote worker-kill --maxCount 1 --command OOME \n\n"
                + "# kill one member using bash command kill -9\n"
                + "coordinator-remote worker-kill --maxCount 1 --command 'bash:kill -9 $PID'\n\n"
                + "# kill one member using javascript which calls System.exit\n"
                + "coordinator-remote worker-kill --maxCount 1 --command 'js:java.lang.System.exit(0);'";

        private final OptionSpec<String> commandSpec = parser.accepts("command",
                "The way to kill the worker. E.g. 'System.exit', 'OOME', 'bash:kill -9 $PID', 'js:somescript")
                .withRequiredArg().ofType(String.class).defaultsTo("System.exit");

        @Override
        protected OptionSet newOptions(String[] args) {
            maxCountSpec.defaultsTo(1);
            workerTypeSpec.defaultsTo("member");
            return initOptionsOnlyWithHelp(parser, help, args);
        }

        @Override
        protected SimulatorOperation newOperation() {
            String command = loadCommand();

            return new RcWorkerKillOperation(command, newQuery());
        }

        private String loadCommand() {
            String command = options.valueOf(commandSpec);
            if ("System.exit".equals(command)) {
                command = "js:java.lang.System.exit(0);";
            } else if ("OOME".equals(command)) {
                command = "var list = new java.util.ArrayList();\n"
                        + "while(true){\n"
                        + "    try{"
                        + "         list.add( new Array(10000));"
                        + "    }catch(error){}"
                        + "}";
            }
            return command;
        }
    }

    private class PrintClusterLayoutCli extends AbstractCli {

        private final String help
                = "Prints the cluster layout on the coordinator.\n";

        @Override
        protected OptionSet newOptions(String[] args) {
            return initOptionsOnlyWithHelp(parser, help, args);
        }

        @Override
        protected SimulatorOperation newOperation() {
            return new RcPrintLayoutOperation();
        }
    }

    private class ExitCli extends AbstractCli {

        private final String help
                = "Terminates the the coordinator session.\n";

        @Override
        protected OptionSet newOptions(String[] args) {
            return initOptionsOnlyWithHelp(parser, help, args);
        }

        @Override
        protected SimulatorOperation newOperation() {
            LOGGER.info("Shutting down Coordinator Remote");
            return new RcStopCoordinatorOperation();
        }
    }

    private class WorkerStartCli extends AbstractCli {

        private final String help
                = "The 'worker-start' command starts one or more workers.\n"
                + "\n"
                + "Before a test run run, the appropriate workers need to be started using 'worker-starts'\n"
                + "\n"
                + "By default the workers will be spread so that the number of worker on each agent is in balance.\n"
                + "Using the coordinator --dedicatedMemberMachines setting dedicated member agents can be created.\n"
                + "\n"
                + "The worker-starts command will NOT install software when a --versionSpec is used. Make sure that\n"
                + "appropriate calls to the install command have been made easier.\n"
                + "\n"
                + "A worker can be configured, just like an agent, with tags. This can be used for looking up certain\n"
                + "workers e.g. for running a test, or to pass key/values to configuration script. An worker will\n"
                + "inherit the tags of the agent it runs on and its own tags are added.\n"
                + " \n"
                + "A worker can assigned to a particular agent by making use of the --agent option where a list of\n"
                + "agent simulator adresses is passed, e.g. --agent C_A1. Or by making use of the --agentTags. For\n"
                + "example --agentTags cluster=wan1.\n"
                + "\n"
                + "Examples\n"
                + "# starts 1 members\n"
                + "coordinator-remote worker-start\n\n"
                + "# starts 2 java clients\n"
                + "coordinator-remote worker-start --count 2 --workerType javaclient\n\n"
                + "# starts 3 litemembers using version spec git=master clients\n"
                + "coordinator-remote worker-start --count --workerType litemember --versionSpec git=master\n\n"
                + "# starts 1 member on agent C_A1\n"
                + "coordinator-remote worker-start --agents C_A1 \n\n"
                + "# starts 1 member on agent with tag cluster=wan1\n"
                + "coordinator-remote worker-start --agentTags cluster=wan1 \n\n"
                + "# starts 1 member on agent C_A1 with tags cluster and password=123\n"
                + "coordinator-remote worker-start --tags cluster,password=123 \n\n"
                + "# starts 1 client with a custom client-hazelcast.xml file\n"
                + "coordinator-remote worker-start --config client-hazelcast.xml\n\n";

        private final OptionSpec<String> vmOptionsSpec = parser.accepts("vmOptions",
                "Worker JVM options (quotes can be used).")
                .withRequiredArg().ofType(String.class).defaultsTo("");

        private final OptionSpec<String> versionSpecSpec = parser.accepts("versionSpec",
                "The versionSpec of the member, e.g. maven=3.7. It will default to what is configured in the"
                        + " simulator.properties.")
                .withRequiredArg().ofType(String.class);

        private final OptionSpec<String> workerTypeSpec = parser.accepts("workerType",
                "The type of machine to start. member, litemember, javaclient (native clients will be added soon) etc.")
                .withRequiredArg().ofType(String.class).defaultsTo("member");

        private final OptionSpec<Integer> countSpec = parser.accepts("count",
                "The number of workers to start.")
                .withRequiredArg().ofType(Integer.class).defaultsTo(1);

        private final OptionSpec<String> configSpec = parser.accepts("config",
                "The file containing the configuration to use to start up the worker. E.g. Hazelcast configuration.")
                .withRequiredArg().ofType(String.class);

        private final OptionSpec<String> agentsSpec = parser.accepts("agents",
                "Comma separated list of agents the workers can start on. By default all agents are acceptable")
                .withRequiredArg().ofType(String.class);

        private final OptionSpec<String> agentsTags = parser.accepts("agentTags",
                "Required tags of the agent the worker can be created on")
                .withRequiredArg().ofType(String.class);

        private final OptionSpec<String> tagsSpec = parser.accepts("tags",
                "Comma separated list of key value pairs.")
                .withRequiredArg().ofType(String.class);

        @Override
        protected OptionSet newOptions(String[] args) {
            return initOptionsOnlyWithHelp(parser, help, args);
        }

        @Override
        protected SimulatorOperation newOperation() {
            int count = options.valueOf(countSpec);
            if (count <= 0) {
                throw new CommandLineExitException("--count can't be smaller than 1");
            }

            LOGGER.info(format("Starting %s workers", count));

            String hzConfig = null;
            if (options.has(configSpec)) {
                hzConfig = fileAsText(options.valueOf(configSpec));
            }

            return new RcWorkerStartOperation()
                    .setCount(count)
                    .setVersionSpec(options.valueOf(versionSpecSpec))
                    .setWorkerType(options.valueOf(workerTypeSpec))
                    .setHzConfig(hzConfig)
                    .setVmOptions(options.valueOf(vmOptionsSpec))
                    .setAgentAddresses(loadAddresses(options, agentsSpec, AddressLevel.AGENT))
                    .setTags(TagUtils.loadTags(options, tagsSpec))
                    .setAgentTags(TagUtils.loadTags(options, agentsTags));
        }
    }

    private abstract class TestRunStartCli extends WorkerQueryableCli {

        final OptionSpec<String> durationSpec = parser.accepts("duration",
                "Amount of time to execute the RUN phase per test, e.g. 10s, 1m, 2h or 3d. If duration is set to 0, "
                        + "the test will run until the test decides to stop.")
                .withRequiredArg().ofType(String.class).defaultsTo(format("%ds", DEFAULT_DURATION_SECONDS));

        final OptionSpec<String> warmupSpec = parser.accepts("warmup",
                "Amount of time to execute the warmup per test, e.g. 10s, 1m, 2h or 3d. If warmup is set to 0, "
                        + "the test will warmup until the test decides to stop.")
                .withRequiredArg().ofType(String.class);

        final OptionSpec<TargetType> targetTypeSpec = parser.accepts("targetType",
                format("Defines the type of Workers which execute the RUN phase."
                        + " The type PREFER_CLIENT selects client Workers if they are available, member Workers otherwise."
                        + " List of allowed types: %s", TargetType.getIdsAsString()))
                .withRequiredArg().ofType(TargetType.class).defaultsTo(TargetType.PREFER_CLIENT);

        final OptionSpec parallelSpec = parser.accepts("parallel",
                "If defined tests are run in parallel.");

        final OptionSpec<Boolean> verifyEnabledSpec = parser.accepts("verify",
                "Defines if tests are verified.")
                .withRequiredArg().ofType(Boolean.class).defaultsTo(true);

        final OptionSpec<Boolean> failFastSpec = parser.accepts("failFast",
                "Defines if the TestSuite should fail immediately when a test from a TestSuite fails instead of continuing.")
                .withRequiredArg().ofType(Boolean.class).defaultsTo(true);

        @Override
        WorkerQuery newQuery() {
            WorkerQuery query = super.newQuery();
            if (query.getWorkerAddresses() == null) {
                query.setTargetType(options.valueOf(targetTypeSpec));
            }
            return query;
        }

        @Override
        protected SimulatorOperation newOperation() {
            List testsuiteFiles = options.nonOptionArguments();
            File testSuiteFile;
            if (testsuiteFiles.size() > 1) {
                throw new CommandLineExitException(format("Too many TestSuite files specified: %s", testsuiteFiles));
            } else if (testsuiteFiles.size() == 1) {
                testSuiteFile = new File((String) testsuiteFiles.get(0));
            } else {
                testSuiteFile = new File("test.properties");
            }

            LOGGER.info("File: " + testSuiteFile);

            int durationSeconds = getDurationSeconds(options, durationSpec);
            int warmupSeconds = getDurationSeconds(options, warmupSpec);
            if (durationSeconds != 0 && warmupSeconds > durationSeconds) {
                throw new CommandLineExitException("warmup can't be larger than duration");
            }
            TestSuite suite = new TestSuite(testSuiteFile)
                    .setDurationSeconds(durationSeconds)
                    .setWarmupSeconds(warmupSeconds)
                    .setWorkerQuery(newQuery())
                    .setParallel(options.has(parallelSpec))
                    .setVerifyEnabled(options.valueOf(verifyEnabledSpec))
                    .setFailFast(options.valueOf(failFastSpec));

            if (options.has(warmupSpec)) {
                suite.setWarmupSeconds(getDurationSeconds(options, warmupSpec));
            }

            LOGGER.info("Running testSuite: " + testSuiteFile.getAbsolutePath());
            return new RcTestRunOperation(suite, isAsync(), newQuery());
        }

        abstract boolean isAsync();
    }

    private class TestRunCli extends TestRunStartCli {

        private final String help
                = "The 'test-run' command runs a test suite and waits for its completion.\n"
                + "\n"
                + "A testsuite can contain a single test, or multiple tests when using test4@someproperty=10\n"
                + "By default the test are run in sequential, but can be controlled using the --parallel flag\n"
                + "\n"
                + "By default a test will prefer to run on client and of none available, it will try to run on\n"
                + "The members. Also it will use either all clients or all members as drivers of the test. This\n"
                + "behavior can be controlled using the --targetType and --targetCount options"
                + "\n"
                + "Examples\n"
                + "# runs a file 'test.properties' for 1 minute\n"
                + "coordinator-remote run\n\n"
                + "# runs atomiclong.properties for 1 minute\n"
                + "coordinator-remote run atomiclong.properties\n\n"
                + "# runs a test with a warmup period of 5 minute and a duration of 1 hour\n"
                + "coordinator-remote run --warmup 5m --duration 1h\n\n"
                + "# runs a test by running all tests in the suite in parallel for 10m.\n"
                + "coordinator-remote run --duration 10m --parallel suite.properties\n\n"
                + "# run a test but disable the verification\n"
                + "coordinator-remote run --verify false\n\n"
                + "# run a test but disable the fail fast mechanism\n"
                + "coordinator-remote run --failFast \n\n"
                + "# runs a test on 3 members no matter if there are clients or more than 3 members in the cluster.\n"
                + "coordinator-remote run --targetType member --targetCount 3 \n\n";


        @Override
        protected OptionSet newOptions(String[] args) {
            return initOptionsWithHelp(parser, help, args);
        }

        @Override
        boolean isAsync() {
            return false;
        }
    }

    private class TestStartCli extends TestRunStartCli {

        private final String help
                = "The 'test=start' command runs a test suite asynchronously and returns the id's of the created tests.\n"
                + "\n"
                + "A testsuite can contain a single test, or multiple tests when using test4@someproperty=10\n"
                + "By default the test are run in sequential, but can be controlled using the --parallel flag\n"
                + "\n"
                + "By default a test will prefer to run on client and of none available, it will try to run on\n"
                + "The members. Also it will use either all clients or all members as drivers of the test. This\n"
                + "behavior can be controlled using the --targetType and --targetCount options"
                + "\n"
                + "Examples\n"
                + "# runs a file 'test.properties' for 1 minute\n"
                + "coordinator-remote test-start\n\n"
                + "# runs atomiclong.properties for 1 minute\n"
                + "coordinator-remote test-start atomiclong.properties\n\n"
                + "# runs a test with a warmup period of 5 minute and a duration of 1 hour\n"
                + "coordinator-remote test-start --warmup 5m --duration 1h\n\n"
                + "# runs a test by running all tests in the suite in parallel for 10m.\n"
                + "coordinator-remote test-start --duration 10m --parallel suite.properties\n\n"
                + "# run a test but disable the verification\n"
                + "coordinator-remote test-start --verify false\n\n"
                + "# run a test but disable the fail fast mechanism\n"
                + "coordinator-remote test-start --failFast \n\n"
                + "# runs a test on 3 members no matter if there are clients or more than 3 members in the cluster.\n"
                + "coordinator-remote test-start --targetType member --targetCount 3 \n\n";

        @Override
        protected OptionSet newOptions(String[] args) {
            return initOptionsWithHelp(parser, help, args);
        }

        @Override
        boolean isAsync() {
            return true;
        }
    }

    private static List<String> loadAddresses(OptionSet options, OptionSpec<String> spec, AddressLevel addressLevel) {
        String addresses = options.valueOf(spec);
        if (addresses == null) {
            return null;
        }

        List<String> result = new LinkedList<String>();
        for (String addressString : addresses.split(",")) {

            if (addressString != null) {
                SimulatorAddress address;
                try {
                    address = SimulatorAddress.fromString(addressString);
                } catch (Exception e) {
                    throw new CommandLineExitException("Worker address [" + addressString
                            + "] is not a valid simulator address", e);
                }

                if (!address.getAddressLevel().equals(addressLevel)) {
                    throw new CommandLineExitException("address [" + addressString
                            + "] is not a valid " + addressLevel + " address, it's a " + address.getAddressLevel() + " address");
                }
            }
            result.add(addressString);
        }
        return result;
    }
}
