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
import com.hazelcast.simulator.common.WorkerType;
import com.hazelcast.simulator.coordinator.tasks.ArtifactCleanTask;
import com.hazelcast.simulator.coordinator.tasks.ArtifactDownloadTask;
import com.hazelcast.simulator.protocol.registry.ComponentRegistry;
import com.hazelcast.simulator.protocol.registry.TargetType;
import com.hazelcast.simulator.protocol.registry.WorkerQuery;
import com.hazelcast.simulator.utils.CommandLineExitException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.log4j.Logger;

import java.io.File;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.simulator.common.GitInfo.getBuildTime;
import static com.hazelcast.simulator.common.GitInfo.getCommitIdAbbrev;
import static com.hazelcast.simulator.coordinator.DeploymentPlan.createDeploymentPlan;
import static com.hazelcast.simulator.utils.CliUtils.initOptionsWithHelp;
import static com.hazelcast.simulator.utils.CloudProviderUtils.isLocal;
import static com.hazelcast.simulator.utils.CommonUtils.exitWithError;
import static com.hazelcast.simulator.utils.CommonUtils.getSimulatorVersion;
import static com.hazelcast.simulator.utils.FileUtils.fileAsText;
import static com.hazelcast.simulator.utils.FileUtils.getConfigurationFile;
import static com.hazelcast.simulator.utils.FileUtils.getFileAsTextFromWorkingDirOrBaseDir;
import static com.hazelcast.simulator.utils.FileUtils.getSimulatorHome;
import static com.hazelcast.simulator.utils.FileUtils.getUserDir;
import static com.hazelcast.simulator.utils.HazelcastUtils.initClientHzConfig;
import static com.hazelcast.simulator.utils.HazelcastUtils.initMemberHzConfig;
import static com.hazelcast.simulator.utils.SimulatorUtils.loadComponentRegister;
import static com.hazelcast.simulator.utils.SimulatorUtils.loadSimulatorProperties;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

@SuppressWarnings("FieldCanBeLocal")
final class CoordinatorCli {
    static final int DEFAULT_DURATION_SECONDS = 0;
    private static final int DEFAULT_WORKER_PERFORMANCE_MONITOR_INTERVAL_SECONDS = 10;

    private static final Logger LOGGER = Logger.getLogger(CoordinatorCli.class);

    CoordinatorRunMonolith runMonolith;
    Coordinator coordinator;
    TestSuite testSuite;
    CoordinatorParameters coordinatorParameters;
    ComponentRegistry componentRegistry;
    Map<WorkerType, WorkerParameters> workerParametersMap;
    SimulatorProperties simulatorProperties;
    DeploymentPlan deploymentPlan;

    private final OptionParser parser = new OptionParser();

    private final OptionSpec<Integer> workerVmStartupDelayMsSpec = parser.accepts("workerVmStartupDelayMs",
            "Amount of time in milliseconds to wait between starting up the next member. This is useful to prevent"
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
            .withRequiredArg().ofType(String.class).defaultsTo(WorkerType.JAVA_CLIENT.name());

    private final OptionSpec<Integer> targetCountSpec = parser.accepts("targetCount",
            "Defines the number of Workers which execute the RUN phase. The value 0 selects all Workers.")
            .withRequiredArg().ofType(Integer.class).defaultsTo(0);

    private final OptionSpec<Boolean> autoCreateHzInstanceSpec = parser.accepts("autoCreateHzInstances",
            "Auto create Hazelcast instances.")
            .withRequiredArg().ofType(Boolean.class).defaultsTo(true);

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
            "Downloads all worker artifacts");

    private final OptionSpec cleanSpec = parser.accepts("clean",
            "Cleans the remote Worker directories on the provisioned machines.");

    private final OptionSet options;

    CoordinatorCli(String[] args) {
        this.options = initOptionsWithHelp(parser, args);

        this.simulatorProperties = loadSimulatorProperties();
        this.componentRegistry = newComponentRegistry(simulatorProperties);

        if (!(options.has(downloadSpec) || options.has(cleanSpec))) {
            this.coordinatorParameters = loadCoordinatorParameters();
            this.coordinator = new Coordinator(componentRegistry, coordinatorParameters);

            this.testSuite = loadTestSuite();

            if (testSuite == null) {
                int coordinatorPort = simulatorProperties.getCoordinatorPort();
                if (coordinatorPort == 0) {
                    throw new CommandLineExitException("Can't run without a testSuite, and not have a coordinator port enabled."
                            + "Please add COORDINATOR_PORT=5000 to your simulator.properties or run with a testsuite.");
                }
            } else {
                this.workerParametersMap = loadWorkerParameters();
                this.deploymentPlan = newDeploymentPlan();
                this.runMonolith = new CoordinatorRunMonolith(coordinator, coordinatorParameters);
            }
        }
    }

    void run() throws Exception {
        if (options.has(downloadSpec)) {
            new ArtifactDownloadTask("*", simulatorProperties, getUserDir(), componentRegistry).run();
        } else if (options.has(cleanSpec)) {
            new ArtifactCleanTask(componentRegistry, simulatorProperties).run();
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
                .setAfterCompletionFile(getConfigurationFile("after-completion.sh").getAbsolutePath())
                .setPerformanceMonitorIntervalSeconds(getPerformanceMonitorInterval())
                .setSkipDownload(options.has(skipDownloadSpec))
                .setWorkerVmStartupDelayMs(options.valueOf(workerVmStartupDelayMsSpec))
                .setLicenseKey(options.valueOf(licenseKeySpec));

        if (options.has(sessionIdSpec)) {
            coordinatorParameters.setSessionId(options.valueOf(sessionIdSpec));
        }

        return coordinatorParameters;
    }

    private Map<WorkerType, WorkerParameters> loadWorkerParameters() {
        Map<WorkerType, WorkerParameters> result = new HashMap<WorkerType, WorkerParameters>();
        result.put(WorkerType.MEMBER, loadMemberWorkerParameters());
        result.put(WorkerType.LITE_MEMBER, loadLiteMemberWorkerParameters());
        result.put(WorkerType.JAVA_CLIENT, loadJavaClientWorkerParameters());
        return result;
    }

    private WorkerParameters loadJavaClientWorkerParameters() {
        Map<String, String> env = new HashMap<String, String>();
        env.put("AUTOCREATE_HAZELCAST_INSTANCE", "" + options.valueOf(autoCreateHzInstanceSpec));
        env.put("LOG4j_CONFIG", loadLog4jConfig());
        env.put("JVM_OPTIONS", loadClientArgs());
        env.put("WORKER_PERFORMANCE_MONITOR_INTERVAL_SECONDS",
                Integer.toString(coordinatorParameters.getPerformanceMonitorIntervalSeconds()));
        env.put("HAZELCAST_CONFIG",
                initClientHzConfig(
                        loadClientHzConfig(),
                        componentRegistry,
                        simulatorProperties.asMap(),
                        coordinatorParameters.getLicenseKey()));

        return new WorkerParameters()
                .setVersionSpec(simulatorProperties.getVersionSpec())
                .setWorkerStartupTimeout(simulatorProperties.getWorkerStartupTimeoutSeconds())
                .setWorkerScript(loadWorkerScript(WorkerType.JAVA_CLIENT, simulatorProperties.get("VENDOR")))
                .setEnvironment(env);
    }

    private WorkerParameters loadLiteMemberWorkerParameters() {
        Map<String, String> env = new HashMap<String, String>();
        env.put("AUTOCREATE_HAZELCAST_INSTANCE", "" + options.valueOf(autoCreateHzInstanceSpec));
        env.put("LOG4j_CONFIG", loadLog4jConfig());
        env.put("JVM_OPTIONS", loadClientArgs());
        env.put("WORKER_PERFORMANCE_MONITOR_INTERVAL_SECONDS",
                Integer.toString(coordinatorParameters.getPerformanceMonitorIntervalSeconds()));
        env.put("HAZELCAST_CONFIG",
                initMemberHzConfig(loadMemberHzConfig(),
                        componentRegistry,
                        coordinatorParameters.getLicenseKey(),
                        simulatorProperties.asMap(), true));

        return new WorkerParameters()
                .setVersionSpec(simulatorProperties.getVersionSpec())
                .setWorkerStartupTimeout(simulatorProperties.getWorkerStartupTimeoutSeconds())
                .setWorkerScript(loadWorkerScript(WorkerType.LITE_MEMBER, simulatorProperties.get("VENDOR")))
                .setEnvironment(env);
    }

    private WorkerParameters loadMemberWorkerParameters() {
        Map<String, String> env = new HashMap<String, String>();
        env.put("AUTOCREATE_HAZELCAST_INSTANCE", "" + options.valueOf(autoCreateHzInstanceSpec));
        env.put("LOG4j_CONFIG", loadLog4jConfig());
        env.put("JVM_OPTIONS", loadMemberArgs());
        env.put("WORKER_PERFORMANCE_MONITOR_INTERVAL_SECONDS",
                Integer.toString(coordinatorParameters.getPerformanceMonitorIntervalSeconds()));
        env.put("HAZELCAST_CONFIG",
                initMemberHzConfig(
                        loadMemberHzConfig(),
                        componentRegistry,
                        coordinatorParameters.getLicenseKey(),
                        simulatorProperties.asMap(), false));

        return new WorkerParameters()
                .setVersionSpec(simulatorProperties.getVersionSpec())
                .setWorkerStartupTimeout(simulatorProperties.getWorkerStartupTimeoutSeconds())
                .setWorkerScript(loadWorkerScript(WorkerType.MEMBER, simulatorProperties.get("VENDOR")))
                .setEnvironment(env);
    }

    private String loadMemberArgs() {
        String args;
        if (options.has(workerVmOptionsSpec)) {
            args = options.valueOf(workerVmOptionsSpec);
            LOGGER.warn("'workerVmOptions' is deprecated, use 'workerArgs' instead.");
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

    private int getPerformanceMonitorInterval() {
        String intervalSeconds = simulatorProperties.get("WORKER_PERFORMANCE_MONITOR_INTERVAL_SECONDS");
        if (intervalSeconds == null || intervalSeconds.isEmpty()) {
            return DEFAULT_WORKER_PERFORMANCE_MONITOR_INTERVAL_SECONDS;
        }
        return Integer.parseInt(intervalSeconds);
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

    private ComponentRegistry newComponentRegistry(SimulatorProperties simulatorProperties) {
        ComponentRegistry componentRegistry;
        if (isLocal(simulatorProperties)) {
            componentRegistry = new ComponentRegistry();
            componentRegistry.addAgent("localhost", "localhost");
        } else {
            componentRegistry = loadComponentRegister(getAgentsFile());
        }

        if (options.has(dedicatedMemberMachinesSpec)) {
            componentRegistry.assignDedicatedMemberMachines(options.valueOf(dedicatedMemberMachinesSpec));
        }

        return componentRegistry;
    }

    private DeploymentPlan newDeploymentPlan() {
        WorkerType workerType = new WorkerType(options.valueOf(clientTypeSpec));
        if (workerType.isMember()) {
            throw new CommandLineExitException("client workerType can't be [member]");
        }

        int members = options.valueOf(membersSpec);
        int clients = options.valueOf(clientsSpec);
        if (clients < 0) {
            throw new CommandLineExitException("--client must be a equal or larger than 0");
        }
        return createDeploymentPlan(componentRegistry, workerParametersMap, workerType, members, clients);
    }


    private static File getAgentsFile() {
        File file = new File("agents.txt");
        if (!file.exists()) {
            throw new CommandLineExitException(format("Agents file [%s] does not exist", file));
        }

        LOGGER.info("Loading Agents file: " + file.getAbsolutePath());
        return file;
    }

    public static String loadWorkerScript(WorkerType workerType, String vendor) {
        List<File> files = new LinkedList<File>();
        File confDir = new File(getSimulatorHome(), "conf");

        files.add(new File("worker-" + vendor + "-" + workerType.name() + ".sh").getAbsoluteFile());
        files.add(new File("worker-" + workerType + ".sh").getAbsoluteFile());
        files.add(new File("worker-" + vendor + ".sh").getAbsoluteFile());
        files.add(new File("worker.sh").getAbsoluteFile());

        files.add(new File(confDir, "worker-" + vendor + "-" + workerType.name() + ".sh").getAbsoluteFile());
        files.add(new File(confDir, "worker-" + vendor + ".sh").getAbsoluteFile());
        files.add(new File(confDir, "worker.sh").getAbsoluteFile());

        for (File file : files) {
            if (file.exists()) {
                LOGGER.info("Loading " + vendor + " " + workerType.name() + " worker script: " + file.getAbsolutePath());
                return fileAsText(file);
            }
        }

        throw new CommandLineExitException("Failed to load worker script from the following locations:" + files);
    }

    public static String loadMemberHzConfig() {
        File file = getConfigurationFile("hazelcast.xml");
        LOGGER.info("Loading Hazelcast member configuration: " + file.getAbsolutePath());
        return fileAsText(file);
    }

    public static String loadClientHzConfig() {
        File file = getConfigurationFile("client-hazelcast.xml");
        LOGGER.info("Loading Hazelcast client configuration: " + file.getAbsolutePath());
        return fileAsText(file);
    }

    public static String loadLog4jConfig() {
        return getFileAsTextFromWorkingDirOrBaseDir(getSimulatorHome(), "worker-log4j.xml", "Log4j configuration for Worker");
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
