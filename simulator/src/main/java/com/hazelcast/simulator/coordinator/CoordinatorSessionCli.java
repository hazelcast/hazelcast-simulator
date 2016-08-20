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
import com.hazelcast.simulator.protocol.core.Response;
import com.hazelcast.simulator.protocol.core.ResponseType;
import com.hazelcast.simulator.protocol.operation.InstallVendorOperation;
import com.hazelcast.simulator.protocol.operation.RunSuiteOperation;
import com.hazelcast.simulator.protocol.operation.ShutdownCoordinatorOperation;
import com.hazelcast.simulator.protocol.operation.StartWorkersOperation;
import com.hazelcast.simulator.protocol.registry.TargetType;
import com.hazelcast.simulator.utils.BashCommand;
import com.hazelcast.simulator.utils.CommandLineExitException;
import com.hazelcast.simulator.utils.FileUtils;
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
import static com.hazelcast.simulator.utils.CliUtils.initOptionsWithHelp;
import static com.hazelcast.simulator.utils.CommonUtils.closeQuietly;
import static com.hazelcast.simulator.utils.CommonUtils.exitWithError;
import static com.hazelcast.simulator.utils.FileUtils.getSimulatorHome;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * todo:
 * - Start should not be done async. The call should wait till the starting has actually completed.
 * - Stopping doesn't complete
 * - Option to kill members
 * - Option to start clients
 * - starting session improvements
 * - stopping session improvements
 * - the worker jvm version spec should default to what is in the simulator.properties
 * - starting light members
 * - help in case there is a problem with parsing the main command
 * - start member; controlling configuration
 * - start client; controlling configuration
 * - cancel running test
 * - cancel all running tests
 * - scaling up down workers
 * - killing random member
 * - Coordinator Session install vendor : parsing + help
 * - when invalid version is used in install; no propper feedback
 * - appearently hazelcast 3.6 is automatically installed on interactive startup
 * - monitor performance
 *
 * nice to have
 * - chaos monkeys
 *
 * done
 * - problem starting members
 */
public class CoordinatorSessionCli implements Closeable {

    private static final Logger LOGGER = Logger.getLogger(CoordinatorSessionCli.class);

    private final SimulatorProperties simulatorProperties;
    private final String cmd;
    private final String[] subArgs;
    private final int coordinatorPort;

    private RemoteControllerConnector connector;

    public CoordinatorSessionCli(String[] args) {
        cmd = args[0];
        subArgs = removeFirst(args);
        simulatorProperties = new SimulatorProperties();
        File file = new File(FileUtils.getUserDir(), "simulator.properties");
        if (file.exists()) {
            simulatorProperties.init(file);
        }

        coordinatorPort = simulatorProperties.getCoordinatorPort();
        if (coordinatorPort == 0) {
            throw new CommandLineExitException("Coordinator port is disabled!");
        }
    }

    public void run() {
        if ("start".equals(cmd)) {
            LOGGER.info("Starting Coordinator Session on port:" + coordinatorPort);
            new BashCommand(getSimulatorHome() + "/bin/" + "coordinator --interactive &").execute();
            return;
        }

        connector = new RemoteControllerConnector("localhost", coordinatorPort);
        connector.start();
        Response response;
        if ("stop".equals(cmd)) {
            LOGGER.info("Shutting down Coordinator Session");
            response = connector.write(new ShutdownCoordinatorOperation());
        } else if ("install".equals(cmd)) {
            response = connector.write(InstallVendorCli.newOperation(subArgs));
            System.out.println(response);
        } else if ("start-workers".equals(cmd)) {
            response = connector.write(new StartWorkersCli().newOperation(subArgs));
        } else if ("run".equals(cmd)) {
            response = connector.write(new RunTestCli().newOperation(subArgs));
        } else {
            throw new CommandLineExitException("Unrecognized cmd '" + cmd + "'");
        }

        ResponseType responseType = response.getFirstErrorResponseType();
        if (responseType != ResponseType.SUCCESS) {
            throw new CommandLineExitException("Could not process command: " + responseType);
        }
    }

    @Override
    public void close() {
        closeQuietly(connector);
    }

    public static void main(String[] args) {
        CoordinatorSessionCli cli = null;
        try {
            cli = new CoordinatorSessionCli(args);
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

    private static class InstallVendorCli {
        private final OptionParser parser = new OptionParser();

        static InstallVendorOperation newOperation(String[] args) {
            LOGGER.info("Installing " + args[0]);
            return new InstallVendorOperation(args[0]);
        }
    }

    private class StartWorkersCli {
        private final OptionParser parser = new OptionParser();

        private final OptionSpec<String> vmOptionsSpec = parser.accepts("vmOptions",
                "Worker JVM options (quotes can be used).")
                .withRequiredArg().ofType(String.class).defaultsTo("-XX:+HeapDumpOnOutOfMemoryError");

        private final OptionSpec<String> versionSpecSpec = parser.accepts("versionSpec",
                "The versionSpec of the member, e.g. maven=3.7")
                .withRequiredArg().ofType(String.class).defaultsTo("outofthebox");

        private final OptionSpec<String> workerTypeSpec = parser.accepts("workerType",
                "The type of machine to start. member, litemember, client:java (native clients will be added soon) etc")
                .withRequiredArg().ofType(String.class).defaultsTo("member");

        private OptionSet options;

        StartWorkersOperation newOperation(String[] args) {
            this.options = initOptionsWithHelp(parser, args);

            if (options.nonOptionArguments().size() != 1) {
                throw new CommandLineExitException(format("Too many arguments"));
            }

            int memberCount = Integer.parseInt((String) options.nonOptionArguments().get(0));
            if (memberCount < 0) {
                throw new CommandLineExitException("member count can't be smaller than 0");
            }

            return new StartWorkersOperation(
                    memberCount,
                    options.valueOf(versionSpecSpec),
                    options.valueOf(vmOptionsSpec),
                    options.valueOf(workerTypeSpec),
                    null);
        }
    }

    private class RunTestCli {
        private final OptionParser parser = new OptionParser();

        private final OptionSpec<String> durationSpec = parser.accepts("duration",
                "Amount of time to execute the RUN phase per test, e.g. 10s, 1m, 2h or 3d.")
                .withRequiredArg().ofType(String.class).defaultsTo(format("%ds", DEFAULT_DURATION_SECONDS));

        private final OptionSpec<String> warmupDurationSpec = parser.accepts("warmupDuration",
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

        RunSuiteOperation newOperation(String[] args) {
            this.options = initOptionsWithHelp(parser, args);

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
                    .setWarmupDurationSeconds(getDurationSeconds(warmupDurationSpec))
                    .setTargetType(options.valueOf(targetTypeSpec))
                    .setTargetCount(options.valueOf(targetCountSpec))
                    .setParallel(options.has(parallelSpec))
                    .setVerifyEnabled(options.valueOf(verifyEnabledSpec))
                    .setFailFast(options.has(failFastSpec));

            LOGGER.info("Running testSuite:" + testSuiteFile.getAbsolutePath());
            return new RunSuiteOperation(suite);
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
}
