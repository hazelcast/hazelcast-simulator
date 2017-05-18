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
import com.hazelcast.simulator.common.TestCase;
import com.hazelcast.simulator.common.TestPhase;
import com.hazelcast.simulator.coordinator.registry.Registry;
import com.hazelcast.simulator.coordinator.registry.WorkerQuery;
import com.hazelcast.simulator.coordinator.tasks.ArtifactCleanTask;
import com.hazelcast.simulator.coordinator.tasks.DownloadTask;
import com.hazelcast.simulator.utils.CommandLineExitException;
import com.hazelcast.simulator.vendors.VendorDriver;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.log4j.Logger;

import java.io.File;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.simulator.common.GitInfo.getBuildTime;
import static com.hazelcast.simulator.common.GitInfo.getCommitIdAbbrev;
import static com.hazelcast.simulator.coordinator.AgentUtils.onlineCheckAgents;
import static com.hazelcast.simulator.coordinator.registry.AgentData.publicAddresses;
import static com.hazelcast.simulator.utils.CliUtils.initOptionsWithHelp;
import static com.hazelcast.simulator.utils.CloudProviderUtils.isLocal;
import static com.hazelcast.simulator.utils.CommonUtils.exitWithError;
import static com.hazelcast.simulator.utils.CommonUtils.getSimulatorVersion;
import static com.hazelcast.simulator.utils.FileUtils.getSimulatorHome;
import static com.hazelcast.simulator.utils.FileUtils.getUserDir;
import static com.hazelcast.simulator.utils.SimulatorUtils.loadComponentRegister;
import static com.hazelcast.simulator.utils.SimulatorUtils.loadSimulatorProperties;
import static com.hazelcast.simulator.vendors.VendorDriver.loadVendorDriver;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

@SuppressWarnings("FieldCanBeLocal")
final class CoordinatorCli {
    static final int DEFAULT_DURATION_SECONDS = 0;

    private static final Logger LOGGER = Logger.getLogger(CoordinatorCli.class);

    CoordinatorRunMonolith runMonolith;
    Coordinator coordinator;
    TestSuite testSuite;
    CoordinatorParameters coordinatorParameters;
    Registry registry;
    SimulatorProperties simulatorProperties;
    DeploymentPlan deploymentPlan;
    VendorDriver vendorDriver;

    private final OptionParser parser = new OptionParser();

    private final OptionSpec<Integer> workerVmStartupDelayMsSpec = parser.accepts("workerVmStartupDelayMs",
            "Amount of time in milliseconds to wait between starting up the next worker. This is useful to prevent"
                    + "duplicate connection issues.")
            .withRequiredArg().ofType(Integer.class).defaultsTo(0);

    private final OptionSpec<String> durationSpec = parser.accepts("duration",
            "Amount of time to execute the RUN phase per test, e.g. 10s, 1m, 2h or 3d. If duration is set to 0, "
                    + "the test will run until the test decides to stop.")
            .withRequiredArg().ofType(String.class).defaultsTo(format("%ds", DEFAULT_DURATION_SECONDS));

    private final OptionSpec<String> warmupSpec = parser.accepts("warmup",
            "Amount of time for the warmup period. During the warmup period no throughput/latency metrics are tracked."
                    + "This can be used to give the JIT the time to warmup etc. So if you have a duration of 180 seconds, "
                    + "and a warmup of 30 seconds, only for the last 150 seconds of the run performance information is tracked.")
            .withRequiredArg().ofType(String.class).defaultsTo("0s");

    private final OptionSpec<Integer> membersSpec = parser.accepts("members",
            "Number of cluster member Worker JVMs. If no value is specified and no mixed members are specified,"
                    + " then the number of cluster members will be equal to the number of machines in the agents file.")
            .withRequiredArg().ofType(Integer.class).defaultsTo(-1);

    private final OptionSpec<Integer> clientsSpec = parser.accepts("clients",
            "Number of cluster client Worker JVMs.")
            .withRequiredArg().ofType(Integer.class).defaultsTo(0);

    private final OptionSpec<Integer> dedicatedMemberMachinesSpec = parser.accepts("dedicatedMemberMachines",
            "Controls the number of dedicated member machines. For example when there are 4 machines,"
                    + " 2 members and 9 clients with 1 dedicated member machine defined, then"
                    + " 1 machine gets the 2 members and the 3 remaining machines get 3 clients each.")
            .withRequiredArg().ofType(Integer.class).defaultsTo(0);

    private final OptionSpec<TargetType> targetTypeSpec = parser.accepts("targetType",
            format("Defines the type of Workers which execute the RUN phase."
                    + " The type PREFER_CLIENT selects client Workers if they are available, member Workers otherwise."
                    + " List of allowed types: %s", TargetType.getIdsAsString()))
            .withRequiredArg().ofType(TargetType.class).defaultsTo(TargetType.PREFER_CLIENT);

    private final OptionSpec<String> clientTypeSpec = parser.accepts("clientType",
            "Defines the type of client e.g javaclient, litemember, etc.")
            .withRequiredArg().ofType(String.class).defaultsTo("javaclient");

    private final OptionSpec<Integer> targetCountSpec = parser.accepts("targetCount",
            "Defines the number of Workers which execute the RUN phase. The value 0 selects all Workers.")
            .withRequiredArg().ofType(Integer.class).defaultsTo(0);

    private final OptionSpec<String> sessionIdSpec = parser.accepts("sessionId",
            "Defines the ID of the Session. If not set the actual date will be used."
                    + " The session ID is used for creating the working directory")
            .withRequiredArg().ofType(String.class);

    private final OptionSpec<Boolean> verifyEnabledSpec = parser.accepts("verifyEnabled",
            "Defines if tests are verified.")
            .withRequiredArg().ofType(Boolean.class).defaultsTo(true);

    private final OptionSpec<Boolean> failFastSpec = parser.accepts("failFast",
            "Defines if the TestSuite should fail immediately when a test from a TestSuite fails instead of continuing.")
            .withRequiredArg().ofType(Boolean.class).defaultsTo(true);

    private final OptionSpec parallelSpec = parser.accepts("parallel",
            "If defined tests are run in parallel.");

    private final OptionSpec<TestPhase> syncToTestPhaseSpec = parser.accepts("syncToTestPhase",
            format("Defines the last TestPhase which is synchronized between all parallel running tests."
                    + " Use --syncToTestPhase %s to synchronize all test phases."
                    + " List of defined test phases: %s", TestPhase.getLastTestPhase(), TestPhase.getIdsAsString()))
            .withRequiredArg().ofType(TestPhase.class).defaultsTo(TestPhase.getLastTestPhase());

    private final OptionSpec<String> workerVmOptionsSpec = parser.accepts("workerVmOptions",
            "Member Worker JVM options (quotes can be used). This option is deprecated, use 'memberArgs' instead.")
            .withRequiredArg().ofType(String.class).defaultsTo("-XX:+HeapDumpOnOutOfMemoryError");

    private final OptionSpec<String> memberArgsSpec = parser.accepts("memberArgs",
            "Member Worker JVM options (quotes can be used). ")
            .withRequiredArg().ofType(String.class).defaultsTo("-XX:+HeapDumpOnOutOfMemoryError");

    private final OptionSpec<String> clientArgsSpec = parser.accepts("clientArgs",
            "Client Worker JVM options (quotes can be used).")
            .withRequiredArg().ofType(String.class).defaultsTo("-XX:+HeapDumpOnOutOfMemoryError");

    private final OptionSpec<String> clientWorkerVmOptionsSpec = parser.accepts("clientWorkerVmOptions",
            "Client Worker JVM options (quotes can be used). This option is deprecated, use 'clientArgs' instead.")
            .withRequiredArg().ofType(String.class).defaultsTo("-XX:+HeapDumpOnOutOfMemoryError");

    private final OptionSpec<String> licenseKeySpec = parser.accepts("licenseKey",
            "Sets the license key for Hazelcast Enterprise Edition.")
            .withRequiredArg().ofType(String.class);

    private final OptionSpec skipDownloadSpec = parser.accepts("skipDownload",
            "Prevents downloading of the created worker artifacts.");

    private final OptionSpec downloadSpec = parser.accepts("download",
            "Downloads all worker artifacts and applies a postprocessing.");

    private final OptionSpec cleanSpec = parser.accepts("clean",
            "Cleans the remote Worker directories on the provisioned machines.");

    private final OptionSet options;

    CoordinatorCli(String[] args) {
        this.options = initOptionsWithHelp(parser, args);
        this.simulatorProperties = loadSimulatorProperties()
                .setIfNotNull("LICENCE_KEY", options.valueOf(licenseKeySpec));
        this.registry = newComponentRegistry(simulatorProperties);

        onlineCheckAgents(simulatorProperties, registry);

        if (!(options.has(downloadSpec) || options.has(cleanSpec))) {
            this.coordinatorParameters = loadCoordinatorParameters();
            this.simulatorProperties.set("SESSION_ID", coordinatorParameters.getSessionId());
            this.coordinator = new Coordinator(registry, coordinatorParameters);
            this.vendorDriver = loadVendorDriver(simulatorProperties.get("VENDOR"))
                    .setAll(simulatorProperties.asPublicMap())
                    .setAgents(registry.getAgents())
                    .set("CLIENT_ARGS", loadClientArgs())
                    .set("MEMBER_ARGS", loadMemberArgs());

            this.testSuite = loadTestSuite();

            if (testSuite == null) {
                int coordinatorPort = simulatorProperties.getCoordinatorPort();
                if (coordinatorPort == 0) {
                    throw new CommandLineExitException("Can't run without a testSuite, and not have a coordinator port enabled."
                            + "Please add COORDINATOR_PORT=5000 to your simulator.properties or run with a testsuite.");
                }
            } else {
                this.deploymentPlan = newDeploymentPlan();
                this.runMonolith = new CoordinatorRunMonolith(coordinator, coordinatorParameters);
            }
        }
    }

    void run() throws Exception {
        if (options.has(downloadSpec)) {
            new DownloadTask(publicAddresses(registry.getAgents()), simulatorProperties.asMap(), getUserDir(), "*").run();
        } else if (options.has(cleanSpec)) {
            new ArtifactCleanTask(registry, simulatorProperties).run();
        } else {
            coordinator.start();
            if (testSuite == null) {
                int coordinatorPort = coordinatorParameters.getSimulatorProperties().getCoordinatorPort();
                LOGGER.info("Coordinator started in interactive mode on port " + coordinatorPort + ". "
                        + "Waiting for commands from the coordinator-remote.");
            } else {
                runMonolith.init(deploymentPlan);
                boolean success = runMonolith.run(testSuite);
                System.exit(success ? 0 : 1);
            }
        }
    }

    private CoordinatorParameters loadCoordinatorParameters() {
        CoordinatorParameters coordinatorParameters = new CoordinatorParameters()
                .setSimulatorProperties(simulatorProperties)
                .setLastTestPhaseToSync(options.valueOf(syncToTestPhaseSpec))
                .setSkipDownload(options.has(skipDownloadSpec))
                .setWorkerVmStartupDelayMs(options.valueOf(workerVmStartupDelayMsSpec))
                .setPerformanceMonitorIntervalSeconds(
                        Integer.parseInt(simulatorProperties.get("WORKER_PERFORMANCE_MONITOR_INTERVAL_SECONDS")));

        if (options.has(sessionIdSpec)) {
            coordinatorParameters.setSessionId(options.valueOf(sessionIdSpec));
        }

        return coordinatorParameters;
    }

    private String loadMemberArgs() {
        String args;
        if (options.has(workerVmOptionsSpec)) {
            args = options.valueOf(workerVmOptionsSpec);
            LOGGER.warn("'workerVmOptions' is deprecated, use 'memberArgs' instead.");
        } else {
            args = options.valueOf(memberArgsSpec);
        }
        return args;
    }

    private String loadClientArgs() {
        String args;
        if (options.has(clientWorkerVmOptionsSpec)) {
            args = options.valueOf(clientWorkerVmOptionsSpec);
            LOGGER.warn("'clientWorkerVmOptions' is deprecated, use 'clientArgs' instead.");
        } else {
            args = options.valueOf(clientArgsSpec);
        }
        return args;
    }

    private TestSuite loadTestSuite() {
        TestSuite testSuite = loadRawTestSuite();
        if (testSuite == null) {
            return null;
        }

        WorkerQuery workerQuery = new WorkerQuery()
                .setTargetType(options.valueOf(targetTypeSpec));

        int targetCount = options.valueOf(targetCountSpec);
        if (targetCount > 0) {
            workerQuery.setMaxCount(targetCount);
        }

        int durationSeconds = getDurationSeconds(options, durationSpec);
        int warmupSeconds = getDurationSeconds(options, warmupSpec);
        if (durationSeconds != 0 && warmupSeconds > durationSeconds) {
            throw new CommandLineExitException("warmup can't be larger than duration");
        }

        testSuite.setDurationSeconds(durationSeconds)
                .setWarmupSeconds(warmupSeconds)
                .setFailFast(options.valueOf(failFastSpec))
                .setVerifyEnabled(options.valueOf(verifyEnabledSpec))
                .setParallel(options.has(parallelSpec))
                .setWorkerQuery(workerQuery);

        // if the coordinator is not monitoring performance, we don't care for measuring latencies
        if (coordinatorParameters.getPerformanceMonitorIntervalSeconds() == 0) {
            for (TestCase testCase : testSuite.getTestCaseList()) {
                testCase.setProperty("measureLatency", "false");
            }
        }

        return testSuite;
    }

    private TestSuite loadRawTestSuite() {
        String content;
        List testsuiteFiles = options.nonOptionArguments();
        File defaultTestProperties = new File("test.properties");
        if (testsuiteFiles.size() > 1) {
            throw new CommandLineExitException(format("Too many TestSuite files specified: %s", testsuiteFiles));
        } else if (testsuiteFiles.size() == 1) {
            content = (String) testsuiteFiles.get(0);
        } else if (defaultTestProperties.exists()) {
            content = defaultTestProperties.getPath();
        } else {
            return null;
        }

        TestSuite testSuite;
        File testSuiteFile = new File(content);
        if (testSuiteFile.exists()) {
            LOGGER.info("Loading TestSuite file: " + testSuiteFile.getAbsolutePath());
            testSuite = new TestSuite(testSuiteFile);
        } else if (!content.endsWith(".properties")) {
            testSuite = new TestSuite(content);
        } else {
            throw new CommandLineExitException(format("TestSuite file '%s' not found", testSuiteFile));
        }
        return testSuite;
    }

    public static int getDurationSeconds(OptionSet options, OptionSpec<String> optionSpec) {
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

    public static int parseDurationWithoutLastChar(TimeUnit timeUnit, String value) {
        String sub = value.substring(0, value.length() - 1);
        return (int) timeUnit.toSeconds(Integer.parseInt(sub));
    }

    private Registry newComponentRegistry(SimulatorProperties simulatorProperties) {
        Registry registry;
        if (isLocal(simulatorProperties)) {
            registry = new Registry();
            registry.addAgent("localhost", "localhost");
        } else {
            registry = loadComponentRegister(getAgentsFile());
        }

        if (options.has(dedicatedMemberMachinesSpec)) {
            registry.assignDedicatedMemberMachines(options.valueOf(dedicatedMemberMachinesSpec));
        }

        return registry;
    }

    private DeploymentPlan newDeploymentPlan() {
        String workerType = options.valueOf(clientTypeSpec);
        if ("member".equals(workerType)) {
            throw new CommandLineExitException("client workerType can't be [member]");
        }

        int members = options.valueOf(membersSpec);
        if (members == -1) {
            members = registry.agentCount();
        } else if (members < -1) {
            throw new CommandLineExitException("--member must be a equal or larger than -1");
        }

        int clients = options.valueOf(clientsSpec);
        if (clients < 0) {
            throw new CommandLineExitException("--client must be a equal or larger than 0");
        }

        if (members == 0 && clients == 0) {
            throw new CommandLineExitException("No workers have been defined!");
        }

        DeploymentPlan plan = new DeploymentPlan(vendorDriver, registry.getAgents())
                .addToPlan(members, "member")
                .addToPlan(clients, options.valueOf(clientTypeSpec));
        plan.printLayout();
        return plan;
    }

    private static File getAgentsFile() {
        File file = new File("agents.txt");
        if (!file.exists()) {
            throw new CommandLineExitException(format("Agents file [%s] does not exist", file));
        }

        LOGGER.info("Loading Agents file: " + file.getAbsolutePath());
        return file;
    }

    public static void main(String[] args) {
        LOGGER.info("Hazelcast Simulator Coordinator");
        LOGGER.info(format("Version: %s, Commit: %s, Build Time: %s",
                getSimulatorVersion(), getCommitIdAbbrev(), getBuildTime()));
        LOGGER.info(format("SIMULATOR_HOME: %s", getSimulatorHome().getAbsolutePath()));

        try {
            CoordinatorCli cli = new CoordinatorCli(args);
            cli.run();
        } catch (Exception e) {
            exitWithError(LOGGER, "Failed to run Coordinator", e);
        }
    }
}
