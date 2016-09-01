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
import com.hazelcast.simulator.common.TestSuite;
import com.hazelcast.simulator.protocol.connector.RemoteControllerConnector;
import com.hazelcast.simulator.protocol.core.AddressLevel;
import com.hazelcast.simulator.protocol.core.Response;
import com.hazelcast.simulator.protocol.core.ResponseType;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.operation.RcDownloadOperation;
import com.hazelcast.simulator.protocol.operation.RcInstallOperation;
import com.hazelcast.simulator.protocol.operation.RcKillWorkerOperation;
import com.hazelcast.simulator.protocol.operation.RcPrintLayoutOperation;
import com.hazelcast.simulator.protocol.operation.RcRunSuiteOperation;
import com.hazelcast.simulator.protocol.operation.RcStartWorkerOperation;
import com.hazelcast.simulator.protocol.operation.RcStopCoordinatorOperation;
import com.hazelcast.simulator.protocol.operation.RcWorkerScriptOperation;
import com.hazelcast.simulator.protocol.operation.SimulatorOperation;
import com.hazelcast.simulator.protocol.registry.TargetType;
import com.hazelcast.simulator.protocol.registry.WorkerQuery;
import com.hazelcast.simulator.utils.CommandLineExitException;
import com.hazelcast.simulator.utils.FileUtils;
import joptsimple.NonOptionArgumentSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.log4j.Logger;

import java.io.Closeable;
import java.io.File;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.simulator.coordinator.CoordinatorCli.DEFAULT_DURATION_SECONDS;
import static com.hazelcast.simulator.coordinator.CoordinatorCli.DEFAULT_WARMUP_DURATION_SECONDS;
import static com.hazelcast.simulator.utils.CliUtils.initOptionsOnlyWithHelp;
import static com.hazelcast.simulator.utils.CliUtils.initOptionsWithHelp;
import static com.hazelcast.simulator.utils.CommonUtils.closeQuietly;
import static com.hazelcast.simulator.utils.CommonUtils.exitWithError;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * todo:
 * - Coordinator Remote install vendor : parsing + help
 * - when invalid version is used in install; no proper feedback
 * - if there are no workers, don't show a stacktrace.
 * com.hazelcast.simulator.utils.CommandLineExitException: No workers running!
 * at com.hazelcast.simulator.protocol.registry.ComponentRegistry.getFirstWorker(ComponentRegistry.java:182)
 * at com.hazelcast.simulator.coordinator.RemoteClient.sendToTestOnFirstWorker(RemoteClient.java:93)
 * at com.hazelcast.simulator.coordinator.TestCaseRunner.executePhase(TestCaseRunner.java:198)
 * <p>
 * nice to have
 * - chaos monkeys
 * - cancel running test
 * - cancel all running tests
 * - scaling up down workers
 * <p>
 * done
 */
public class CoordinatorRemoteCli implements Closeable {

    private static final Logger LOGGER = Logger.getLogger(CoordinatorRemoteCli.class);

    private final SimulatorProperties simulatorProperties;
    private final int coordinatorPort;
    private final String[] args;
    private RemoteControllerConnector connector;

    public CoordinatorRemoteCli(String[] args) {
        this.args = args;
        this.simulatorProperties = new SimulatorProperties();
        File file = new File(FileUtils.getUserDir(), "simulator.properties");
        if (file.exists()) {
            simulatorProperties.init(file);
        }

        this.coordinatorPort = simulatorProperties.getCoordinatorPort();
        if (coordinatorPort == 0) {
            throw new CommandLineExitException("Coordinator port is disabled!");
        }
    }

    public void run() {
        if (args.length == 0) {
            printHelpAndExit();
        }

        String cmd = args[0];
        String[] subArgs = removeFirst(args);

        connector = new RemoteControllerConnector("localhost", coordinatorPort);
        connector.start();
        Response response;
        if ("download".equals(cmd)) {
            response = connector.write(new DownloadCli().newOperation(subArgs));
        } else if ("install".equals(cmd)) {
            response = connector.write(new InstallCli().newOperation(subArgs));
        } else if ("kill-worker".equals(cmd)) {
            response = connector.write(new KillWorkerCli().newOperation(subArgs));
        } else if ("print-layout".equals(cmd)) {
            response = connector.write(new PrintClusterLayoutCli().newOperation(subArgs));
        } else if ("run".equals(cmd)) {
            response = connector.write(new RunCli().newOperation(subArgs));
        } else if ("script-worker".equals(cmd)) {
            response = connector.write(new ScriptWorkerCli().newOperation(subArgs));
        } else if ("start-worker".equals(cmd)) {
            response = connector.write(new StartWorkerCli().newOperation(subArgs));
        } else if ("stop".equals(cmd)) {
            response = connector.write(new ExitCli().newOperation(subArgs));
        } else {
            printHelpAndExit();
            return;
        }

        ResponseType responseType = response.getFirstErrorResponseType();
        if (responseType != ResponseType.SUCCESS) {
            throw new CommandLineExitException("Could not process command: " + responseType);
        }
    }

    private static void printHelpAndExit() {
        System.out.println(
                "Command         Description                                                                 \n"
                        + "--------------------------                                                                  \n"
                        + "download        Downloads all artifacts from the workers                                    \n"
                        + "install         Installs vendor software on the remote machines                             \n"
                        + "kill-worker     Kills one or more workers                                                   \n"
                        + "print-layout    Prints the cluster-layout                                                   \n"
                        + "run             Runs a test                                                                 \n"
                        + "script-worker   Executes a script on workers                                                \n"
                        + "start-worker    Starts workers                                                              \n"
                        + "stop            Stops the Coordinator remote session                                        ");
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
            exitWithError(LOGGER, "Failed to run Coordinator", e);
        } finally {
            closeQuietly(cli);
        }
    }

    private static String[] removeFirst(String[] args) {
        String[] result = new String[args.length - 1];
        System.arraycopy(args, 1, result, 0, args.length - 1);
        return result;
    }

    private static class InstallCli {
        private final String help = ""
                + "Install Hazelcast on the agents. By default the coordinator will upload to the agents what has\n"
                + "been configured on the simulator.properties. But in case of testing multiple versions the other\n"
                + "versions need to be installed. If a worker is started without the right software, the worker will\n"
                + "fail to start\n"
                + "\n"
                + "In case of the maven version spec, the local repository is always preferred and no update checking\n"
                + "is done in case of SNAPSHOT version. This is very useful for testing custom SNAPSHOT branches, but\n"
                + "can be a problem if the local SNAPSHOT is not updated and the user expects to use the latest\n"
                + "SNAPSHOT.\n"
                + "\n"
                + "Examples\n"
                + " # installs Hazelcast 3.6 from local or remote repo.\n"
                + "coordinator-remote install maven=3.6\n\n"
                + " # installs Hazelcast 3.8-SNAPSHOT from local or remote repo.\n"
                + "coordinator-remote install maven=3.8-SNAPSHOT\n"
                + " # installs Hazelcast using some git commit hash.\n"
                + "coordinator-remote install git=<somehash>\n";

        private final OptionParser parser = new OptionParser();

        private final NonOptionArgumentSpec argumentSpec = parser
                .nonOptions("version specification");

        private OptionSet options;

        RcInstallOperation newOperation(String[] args) {
            this.options = initOptionsWithHelp(parser, help, args);

            if (options.nonOptionArguments().size() != 1) {
                throw new CommandLineExitException("Too many arguments");
            }

            LOGGER.info("Installing " + args[0]);
            return new RcInstallOperation(args[0]);
        }
    }

    private static class DownloadCli {
        private final String help = ""
                + "The download command downloads all artifacts from the workers.\n";

        private final OptionParser parser = new OptionParser();

        private OptionSet options;

        SimulatorOperation newOperation(String[] args) {
            this.options = initOptionsOnlyWithHelp(parser, help, args);
            return new RcDownloadOperation();
        }
    }

    private static class ScriptWorkerCli {
        private final String help = ""
                + "Executes a script on one or more workers\n"
                + "Various filter options are available like --versionSpec, --workerType, --agentAddress\n"
                + "and it is even possible to execute a script on a specific worker using --workerAddress\n\n"
                + "By default the selection of members is deterministic, however using the --randomSpec setting\n"
                + "one can enable shuffling of members."
                + "\nExamples\n"
                + "# executes takes a threadump on at least 100 members in the cluster one\n"
                + "coordinator-remote script --maxCount 100 --command 'bash:jstack $PID''\n\n"
                + "#executes a javascript that calls System.ext on worker C_A1_W1\n"
                + "coordinator-remote script --workerAddress C_A1_W1 --command 'javascript:java.lang.System.exit(0)'\n";

        private final OptionParser parser = new OptionParser();

        private final OptionSpec<String> versionSpecSpec = parser.accepts("versionSpec",
                "The versionSpec of the member to kill, e.g. maven=3.7. The default value is null, meaning that the versionSpec"
                        + "is not part of the selection criteria ")
                .withRequiredArg().ofType(String.class);

        private final OptionSpec<String> workerTypeSpec = parser.accepts("workerType",
                "The type of machine to kill. member, litemember, client:java (native clients will be added soon) etc")
                .withRequiredArg().ofType(String.class).defaultsTo("member");

        private final OptionSpec<Integer> maxCountSpec = parser.accepts("maxCount",
                "The maximum number of workers to execute the script on. It can safely be called with a maxCount larger than "
                        + "the actual number of workers.")
                .withRequiredArg().ofType(Integer.class).defaultsTo(1);

        private final OptionSpec<String> agentAddressSpec = parser.accepts("agentAddress",
                "The simulator address of the agent owning the worker to kill")
                .withRequiredArg().ofType(String.class);

        private final OptionSpec<String> workerAddressSpec = parser.accepts("workerAddress",
                "The simulator address of the worker to kill")
                .withRequiredArg().ofType(String.class);

        private final OptionSpec<Boolean> randomSpec = parser.accepts("random",
                "If workers should be picked randomly or predictably")
                .withRequiredArg().ofType(Boolean.class).defaultsTo(true);

        private OptionSet options;

        SimulatorOperation newOperation(String[] args) {
            this.options = initOptionsWithHelp(parser, help, args);
            List<?> nonOptionArguments = options.nonOptionArguments();
            if (nonOptionArguments.size() != 1) {
                throw new CommandLineExitException("Only 1 argument allowed. Use single quotes, e.g. 'bash:jstack $PID'");
            }

            String agentAddress = loadAgentAddress(options, agentAddressSpec);
            String workerAddress = loadWorkerAddress(options, workerAddressSpec);
            if (agentAddress != null && workerAddress != null) {
                throw new CommandLineExitException("---agentAddress and --workerAddress can't both be set");
            }

            int maxCount = options.valueOf(maxCountSpec);
            if (maxCount <= 0) {
                throw new CommandLineExitException("--maxCount can't be smaller than 1");
            }

            WorkerQuery workerQuery = new WorkerQuery()
                    .setAgentAddress(agentAddress)
                    .setWorkerAddress(workerAddress)
                    .setWorkerType(options.valueOf(workerTypeSpec))
                    .setVersionSpec(options.valueOf(versionSpecSpec))
                    .setMaxCount(maxCount)
                    .setRandom(options.valueOf(randomSpec));

            String cmd = (String) nonOptionArguments.get(0);
            LOGGER.info("Executing [" + cmd + "]");
            return new RcWorkerScriptOperation(cmd, workerQuery);
        }
    }

    private static class PrintClusterLayoutCli {
        private final String help = ""
                + "Prints the cluster layout on the coordinator.\n";

        private final OptionParser parser = new OptionParser();

        private OptionSet options;

        SimulatorOperation newOperation(String[] args) {
            this.options = initOptionsOnlyWithHelp(parser, help, args);

            return new RcPrintLayoutOperation();
        }
    }

    private static class ExitCli {
        private final String help = ""
                + "Terminates the the coordinator session.\n";

        private final OptionParser parser = new OptionParser();

        private OptionSet options;

        SimulatorOperation newOperation(String[] args) {
            this.options = initOptionsOnlyWithHelp(parser, help, args);
            LOGGER.info("Shutting down Coordinator Remote");
            return new RcStopCoordinatorOperation();
        }
    }

    private static class KillWorkerCli {
        private final String help = ""
                + "The kill-worker command kills one or more workers. The killing can be done based using an exact \n"
                + "worker address or using various filters like versionSpec, etc.\n";

        private final OptionParser parser = new OptionParser();

        private final OptionSpec<String> versionSpecSpec = parser.accepts("versionSpec",
                "The versionSpec of the member to kill, e.g. maven=3.7. The default value is null, meaning that the versionSpec"
                        + "is not part of the selection criteria ")
                .withRequiredArg().ofType(String.class);

        private final OptionSpec<String> workerTypeSpec = parser.accepts("workerType",
                "The type of machine to kill. member, litemember, client:java (native clients will be added soon) etc")
                .withRequiredArg().ofType(String.class).defaultsTo("member");

        private final OptionSpec<Integer> maxCountSpec = parser.accepts("maxCount",
                "The maximum number of workers to kill. It can safely be called with a maxCount larger than "
                        + "the actual number of workers.")
                .withRequiredArg().ofType(Integer.class).defaultsTo(1);

        private final OptionSpec<String> agentAddressSpec = parser.accepts("agentAddress",
                "The simulator address of the agent owning the worker to kill")
                .withRequiredArg().ofType(String.class);

        private final OptionSpec<String> workerAddressSpec = parser.accepts("workerAddress",
                "The simulator address of the worker to kill")
                .withRequiredArg().ofType(String.class);

        private final OptionSpec<String> commandSpec = parser.accepts("command",
                "The way to kill the worker. E.g. 'System.exit', 'OOME', 'bash:kill -9 $PID', 'js:somescript")
                .withRequiredArg().ofType(String.class).defaultsTo("System.exit");

        private final OptionSpec<Boolean> randomSpec = parser.accepts("random",
                "If workers should be picked randomly or predictably")
                .withRequiredArg().ofType(Boolean.class).defaultsTo(true);

        private OptionSet options;

        SimulatorOperation newOperation(String[] args) {
            this.options = initOptionsOnlyWithHelp(parser, help, args);

            int maxCount = options.valueOf(maxCountSpec);
            if (maxCount <= 0) {
                throw new CommandLineExitException("--maxCount can't be smaller than 1");
            }

            String agentAddress = loadAgentAddress(options, agentAddressSpec);
            String workerAddress = loadWorkerAddress(options, workerAddressSpec);
            if (agentAddress != null && workerAddress != null) {
                throw new CommandLineExitException("--agentAddress and --workerAddress can't both be set");
            }

            WorkerQuery workerQuery = new WorkerQuery()
                    .setAgentAddress(agentAddress)
                    .setWorkerAddress(workerAddress)
                    .setWorkerType(options.valueOf(workerTypeSpec))
                    .setVersionSpec(options.valueOf(versionSpecSpec))
                    .setMaxCount(maxCount)
                    .setRandom(options.valueOf(randomSpec));

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
            return new RcKillWorkerOperation(command, workerQuery);
        }
    }

    private class StartWorkerCli {
        private final String help = ""
                + "Starts one or more workers.\n\n"
                + "Before a test run run, the appropriate workers need to be started.\n"
                + "\n"
                + "By default the workers will be spread so that the number of worker on each agent is in balance.\n"
                + "Using the coordinator --dedicatedMemberMachines setting dedicated member agents can be created.\n"
                + "\n"
                + "The start-workers command will NOT install software when a --versionSpec is used. Make sure that\n"
                + "appropriate calls to the install command have been made easier.\n"
                + "Examples\n"
                + "# starts 1 members\n"
                + "coordinator-remote start-worker\n\n"
                + "# starts 2 java clients\n"
                + "coordinator-remote start-worker --count 2 --workerType javaclient\n\n"
                + "# starts 3 litemembers using version spec git=master clients\n"
                + "coordinator-remote start-worker --count --workerType litemember --versionSpec git=master\n\n"
                + "# starts 1 member on agent C_A1\n"
                + "coordinator-remote start-worker --agentAddress C_A1 \n\n"
                + "# starts 1 client with a custom client-hazelcast.xml file\n"
                + "coordinator-remote start-worker --config client-hazelcast.xml \n\n";

        private final OptionParser parser = new OptionParser();

        private final OptionSpec<String> vmOptionsSpec = parser.accepts("vmOptions",
                "Worker JVM options (quotes can be used).")
                .withRequiredArg().ofType(String.class).defaultsTo("");

        private final OptionSpec<String> versionSpecSpec = parser.accepts("versionSpec",
                "The versionSpec of the member, e.g. maven=3.7. It will default to what is configured in the"
                        + " simulator.properties")
                .withRequiredArg().ofType(String.class);

        private final OptionSpec<String> workerTypeSpec = parser.accepts("workerType",
                "The type of machine to start. member, litemember, client:java (native clients will be added soon) etc")
                .withRequiredArg().ofType(String.class).defaultsTo("member");

        private final OptionSpec<Integer> countSpec = parser.accepts("count",
                "The number of workers to start")
                .withRequiredArg().ofType(Integer.class).defaultsTo(1);

        private final OptionSpec<String> configSpec = parser.accepts("config",
                "The file containing the configuration to use to start up the worker. E.g. Hazelcast configuration")
                .withRequiredArg().ofType(String.class);

        private final OptionSpec<String> agentAddressSpec = parser.accepts("agentAddress",
                "The simulator address of the agent to start the worker on")
                .withRequiredArg().ofType(String.class);


        private OptionSet options;

        SimulatorOperation newOperation(String[] args) {
            this.options = initOptionsOnlyWithHelp(parser, help, args);

            int count = options.valueOf(countSpec);
            if (count <= 0) {
                throw new CommandLineExitException("--count can't be smaller than 1");
            }

            LOGGER.info(format("Starting %s workers", count));

            return new RcStartWorkerOperation(
                    count,
                    options.valueOf(versionSpecSpec),
                    options.valueOf(vmOptionsSpec),
                    options.valueOf(workerTypeSpec),
                    options.valueOf(configSpec),
                    loadAgentAddress(options, agentAddressSpec));
        }
    }

    private class RunCli {
        private final String help = "Runs a test";

        private final OptionParser parser = new OptionParser();

        private final OptionSpec<String> durationSpec = parser.accepts("duration",
                "Amount of time to execute the RUN phase per test, e.g. 10s, 1m, 2h or 3d.")
                .withRequiredArg().ofType(String.class).defaultsTo(format("%ds", DEFAULT_DURATION_SECONDS));

        private final OptionSpec<String> warmupSpec = parser.accepts("warmup",
                "Amount of time to execute the warmup per test, e.g. 10s, 1m, 2h or 3d.")
                .withRequiredArg().ofType(String.class).defaultsTo(format("%ds", DEFAULT_WARMUP_DURATION_SECONDS));

        private final OptionSpec<TargetType> targetTypeSpec = parser.accepts("targetType",
                format("Defines the type of Workers which execute the RUN phase."
                        + " The type PREFER_CLIENT selects client Workers if they are available, member Workers otherwise."
                        + " List of allowed types: %s", TargetType.getIdsAsString()))
                .withRequiredArg().ofType(TargetType.class).defaultsTo(TargetType.PREFER_CLIENT);

        private final OptionSpec<Integer> targetCountSpec = parser.accepts("targetCount",
                "Defines the number of Workers which execute the RUN phase. The value 0 selects all Workers.")
                .withRequiredArg().ofType(Integer.class).defaultsTo(0);

        private final OptionSpec parallelSpec = parser.accepts("parallel",
                "If defined tests are run in parallel.");

        private final OptionSpec<Boolean> verifyEnabledSpec = parser.accepts("verifyEnabled",
                "Defines if tests are verified.")
                .withRequiredArg().ofType(Boolean.class).defaultsTo(true);

        private final OptionSpec<Boolean> failFastSpec = parser.accepts("failFast",
                "Defines if the TestSuite should fail immediately when a test from a TestSuite fails instead of continuing.")
                .withRequiredArg().ofType(Boolean.class).defaultsTo(true);

        private OptionSet options;

        SimulatorOperation newOperation(String[] args) {
            this.options = initOptionsWithHelp(parser, help, args);

            List testsuiteFiles = options.nonOptionArguments();
            File testSuiteFile;
            if (testsuiteFiles.size() > 1) {
                throw new CommandLineExitException(format("Too many TestSuite files specified: %s", testsuiteFiles));
            } else if (testsuiteFiles.size() == 1) {
                testSuiteFile = new File((String) testsuiteFiles.get(0));
            } else {
                testSuiteFile = new File("test.properties");
            }

            LOGGER.info("File:" + testSuiteFile);

            TestSuite suite = TestSuite.loadTestSuite(testSuiteFile, "")
                    .setDurationSeconds(getDurationSeconds(durationSpec))
                    .setWarmupSeconds(getDurationSeconds(warmupSpec))
                    .setTargetType(options.valueOf(targetTypeSpec))
                    .setTargetCount(options.valueOf(targetCountSpec))
                    .setParallel(options.has(parallelSpec))
                    .setVerifyEnabled(options.valueOf(verifyEnabledSpec))
                    .setFailFast(options.valueOf(failFastSpec));

            LOGGER.info("Running testSuite:" + testSuiteFile.getAbsolutePath());
            return new RcRunSuiteOperation(suite);
        }

        private int getDurationSeconds(OptionSpec<String> optionSpec) {
            int duration;
            String value = options.valueOf(optionSpec);
            try {
                if (value.endsWith("s")) {
                    duration = parseDurationWithoutLastChar(SECONDS, value);
                } else if (value.endsWith("m")) {
                    duration = parseDurationWithoutLastChar(MINUTES, value);
                } else if (value.endsWith("h")) {
                    duration = parseDurationWithoutLastChar(HOURS, value);
                } else if (value.endsWith("d")) {
                    duration = parseDurationWithoutLastChar(DAYS, value);
                } else {
                    duration = Integer.parseInt(value);
                }
            } catch (NumberFormatException e) {
                throw new CommandLineExitException(format("Failed to parse duration '%s'", value), e);
            }

            if (duration < 0) {
                throw new CommandLineExitException("duration must be a positive number, but was: " + duration);
            }
            return duration;
        }

        private int parseDurationWithoutLastChar(TimeUnit timeUnit, String value) {
            String sub = value.substring(0, value.length() - 1);
            return (int) timeUnit.toSeconds(Integer.parseInt(sub));
        }
    }


    private static String loadAgentAddress(OptionSet options, OptionSpec<String> spec) {
        String agentAddress = options.valueOf(spec);
        if (agentAddress != null) {
            SimulatorAddress address;
            try {
                address = SimulatorAddress.fromString(agentAddress);
            } catch (Exception e) {
                throw new CommandLineExitException("Agent address [" + agentAddress
                        + "] is not a valid simulator address", e);
            }

            if (!address.getAddressLevel().equals(AddressLevel.AGENT)) {
                throw new CommandLineExitException("Agent address [" + agentAddress
                        + "] is not a valid agent address, it's a " + address.getAddressLevel() + " address");
            }
        }
        return agentAddress;
    }

    private static String loadWorkerAddress(OptionSet options, OptionSpec<String> spec) {
        String workerAddress = options.valueOf(spec);
        if (workerAddress != null) {
            SimulatorAddress address;
            try {
                address = SimulatorAddress.fromString(workerAddress);
            } catch (Exception e) {
                throw new CommandLineExitException("Worker address [" + workerAddress
                        + "] is not a valid simulator address", e);
            }

            if (!address.getAddressLevel().equals(AddressLevel.WORKER)) {
                throw new CommandLineExitException("Worker address [" + workerAddress
                        + "] is not a valid worker address, it's a " + address.getAddressLevel() + " address");
            }
        }
        return workerAddress;
    }
}
