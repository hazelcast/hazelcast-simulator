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
import com.hazelcast.simulator.common.TestSuite;
import com.hazelcast.simulator.common.WorkerType;
import com.hazelcast.simulator.protocol.registry.ComponentRegistry;
import com.hazelcast.simulator.protocol.registry.TargetType;
import com.hazelcast.simulator.utils.CommandLineExitException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.log4j.Logger;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.simulator.common.GitInfo.getBuildTime;
import static com.hazelcast.simulator.common.GitInfo.getCommitIdAbbrev;
import static com.hazelcast.simulator.common.SimulatorProperties.PROPERTIES_FILE_NAME;
import static com.hazelcast.simulator.coordinator.DeploymentPlan.createDeploymentPlan;
import static com.hazelcast.simulator.utils.CliUtils.initOptionsWithHelp;
import static com.hazelcast.simulator.utils.CloudProviderUtils.isLocal;
import static com.hazelcast.simulator.utils.CommonUtils.exitWithError;
import static com.hazelcast.simulator.utils.CommonUtils.getSimulatorVersion;
import static com.hazelcast.simulator.utils.FileUtils.fileAsText;
import static com.hazelcast.simulator.utils.FileUtils.getConfigurationFile;
import static com.hazelcast.simulator.utils.FileUtils.getFileAsTextFromWorkingDirOrBaseDir;
import static com.hazelcast.simulator.utils.FileUtils.getSimulatorHome;
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
    static final int DEFAULT_DURATION_SECONDS = 60;
    static final int DEFAULT_WARMUP_DURATION_SECONDS = 0;
    private static final int DEFAULT_WORKER_PERFORMANCE_MONITOR_INTERVAL_SECONDS = 10;

    private static final Logger LOGGER = Logger.getLogger(CoordinatorCli.class);

    final Coordinator coordinator;
    final TestSuite testSuite;
    final CoordinatorParameters coordinatorParameters;
    final ComponentRegistry componentRegistry;
    final Map<WorkerType, WorkerParameters> workerParametersMap;

    private final OptionParser parser = new OptionParser();

    private final OptionSpec<Integer> workerVmStartupDelayMsSpec = parser.accepts("workerVmStartupDelayMs",
            "Amount of time in milliseconds to wait between starting up the next member. This is useful to prevent"
                    + "duplicate connection issues.")
            .withRequiredArg().ofType(Integer.class).defaultsTo(0);

    private final OptionSpec<String> durationSpec = parser.accepts("duration",
            "Amount of time to execute the RUN phase per test, e.g. 10s, 1m, 2h or 3d.")
            .withRequiredArg().ofType(String.class).defaultsTo(format("%ds", DEFAULT_DURATION_SECONDS));

    private final OptionSpec<String> warmupDurationSpec = parser.accepts("warmupDuration",
            "Amount of time to execute the warmup per test, e.g. 10s, 1m, 2h or 3d.")
            .withRequiredArg().ofType(String.class).defaultsTo(format("%ds", DEFAULT_WARMUP_DURATION_SECONDS));

    private final OptionSpec waitForTestCaseSpec = parser.accepts("waitForTestCaseCompletion",
            "Wait for the TestCase to finish its RUN phase. Can be combined with --duration to limit runtime.");

    private final OptionSpec<String> overridesSpec = parser.accepts("overrides",
            "Properties that override the properties in a given test-case, e.g. --overrides"
                    + " \"threadcount=20,writeProb=0.2\". This makes it easy to parametrize a test.")
            .withRequiredArg().ofType(String.class).defaultsTo("");

    private final OptionSpec<Integer> memberWorkerCountSpec = parser.accepts("memberWorkerCount",
            "Number of cluster member Worker JVMs. If no value is specified and no mixed members are specified,"
                    + " then the number of cluster members will be equal to the number of machines in the agents file.")
            .withRequiredArg().ofType(Integer.class).defaultsTo(-1);

    private final OptionSpec<Integer> clientWorkerCountSpec = parser.accepts("clientWorkerCount",
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

    private final OptionSpec<String> workerClassPathSpec = parser.accepts("workerClassPath",
            "A file/directory containing the classes/jars/resources that are going to be uploaded to the agents."
                    + " Use ';' as separator for multiple entries. The wildcard '*' can also be used.")
            .withRequiredArg().ofType(String.class);

    private final OptionSpec<String> sessionIdSpec = parser.accepts("sessionId",
            "Defines the ID of the Session. If not set the actual date will be used."
                    + " The session ID is used for creating the working directory")
            .withRequiredArg().ofType(String.class);

    private final OptionSpec monitorPerformanceSpec = parser.accepts("monitorPerformance",
            "If defined performance of tests is tracked.");

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
            "Member Worker JVM options (quotes can be used).")
            .withRequiredArg().ofType(String.class).defaultsTo("-XX:+HeapDumpOnOutOfMemoryError");

    private final OptionSpec<String> clientWorkerVmOptionsSpec = parser.accepts("clientWorkerVmOptions",
            "Client Worker JVM options (quotes can be used).")
            .withRequiredArg().ofType(String.class).defaultsTo("-XX:+HeapDumpOnOutOfMemoryError");

    private final OptionSpec<String> propertiesFileSpec = parser.accepts("propertiesFile",
            format("The file containing the simulator properties. If no file is explicitly configured,"
                            + " first the working directory is checked for a file '%s'."
                            + " All missing properties are always loaded from SIMULATOR_HOME/conf/%s",
                    PROPERTIES_FILE_NAME, PROPERTIES_FILE_NAME))
            .withRequiredArg().ofType(String.class);

    private final OptionSpec<String> licenseKeySpec = parser.accepts("licenseKey",
            "Sets the license key for Hazelcast Enterprise Edition.")
            .withRequiredArg().ofType(String.class);

    private final OptionSpec skipDownloadSpec = parser.accepts("skipDownload",
            "Prevents downloading of the created worker artifacts.");

    private final OptionSpec remoteSpec = parser.accepts("remote",
            "Puts Coordinator into remote control mode for coordinator-remote");

    private final OptionSet options;
    private final SimulatorProperties simulatorProperties;
    private final DeploymentPlan deploymentPlan;

    CoordinatorCli(String[] args) {
        this.options = initOptionsWithHelp(parser, args);

        this.testSuite = loadTestSuite();

        this.simulatorProperties = loadSimulatorProperties(options, propertiesFileSpec);

        this.componentRegistry = newComponentRegistry(simulatorProperties);

        this.coordinatorParameters = loadCoordinatorParameters();

        this.workerParametersMap = loadWorkerParameters();

        this.deploymentPlan = newDeploymentPlan();

        this.coordinator = new Coordinator(componentRegistry, coordinatorParameters);
    }

    private CoordinatorParameters loadCoordinatorParameters() {
        return new CoordinatorParameters(
                options.valueOf(sessionIdSpec),
                simulatorProperties,
                options.valueOf(workerClassPathSpec),
                options.valueOf(syncToTestPhaseSpec),
                options.valueOf(workerVmStartupDelayMsSpec),
                options.has(skipDownloadSpec),
                getConfigurationFile("after-completion.sh").getAbsolutePath(),
                getPerformanceMonitorInterval());
    }

    private Map<WorkerType, WorkerParameters> loadWorkerParameters() {
        String licenseKey = options.valueOf(licenseKeySpec);
        Map<WorkerType, WorkerParameters> result = new HashMap<WorkerType, WorkerParameters>();
        result.put(WorkerType.MEMBER, loadMemberWorkerParameters(licenseKey));
        result.put(WorkerType.LITE_MEMBER, loadLiteMemberWorkerParameters(licenseKey));
        result.put(WorkerType.JAVA_CLIENT, loadJavaClientWorkerParameters(licenseKey));
        return result;
    }

    private WorkerParameters loadJavaClientWorkerParameters(String licenseKey) {
        Map<String, String> javaClientEnv = new HashMap<String, String>();
        javaClientEnv.put("AUTOCREATE_HAZELCAST_INSTANCE", "" + options.valueOf(autoCreateHzInstanceSpec));
        javaClientEnv.put("LOG4j_CONFIG", loadLog4jConfig());
        javaClientEnv.put("JVM_OPTIONS", options.valueOf(clientWorkerVmOptionsSpec));
        javaClientEnv.put("WORKER_PERFORMANCE_MONITOR_INTERVAL_SECONDS",
                Integer.toString(coordinatorParameters.getPerformanceMonitorIntervalSeconds()));
        javaClientEnv.put("HAZELCAST_CONFIG",
                initClientHzConfig(loadClientHzConfig(), componentRegistry, simulatorProperties.getHazelcastPort(), licenseKey));

        return new WorkerParameters(
                simulatorProperties.getVersionSpec(),
                simulatorProperties.getAsInteger("WORKER_STARTUP_TIMEOUT_SECONDS"),
                loadWorkerScript(WorkerType.JAVA_CLIENT, simulatorProperties.get("VENDOR")),
                javaClientEnv);
    }

    private WorkerParameters loadLiteMemberWorkerParameters(String licenseKey) {
        Map<String, String> liteMemberEnv = new HashMap<String, String>();
        liteMemberEnv.put("AUTOCREATE_HAZELCAST_INSTANCE", "" + options.valueOf(autoCreateHzInstanceSpec));
        liteMemberEnv.put("LOG4j_CONFIG", loadLog4jConfig());
        liteMemberEnv.put("JVM_OPTIONS", options.valueOf(clientWorkerVmOptionsSpec));
        liteMemberEnv.put("WORKER_PERFORMANCE_MONITOR_INTERVAL_SECONDS",
                Integer.toString(coordinatorParameters.getPerformanceMonitorIntervalSeconds()));
        liteMemberEnv.put("HAZELCAST_CONFIG",
                initMemberHzConfig(loadMemberHzConfig(), componentRegistry, simulatorProperties.getHazelcastPort(),
                        licenseKey, simulatorProperties, true));

        return new WorkerParameters(
                simulatorProperties.getVersionSpec(),
                simulatorProperties.getAsInteger("WORKER_STARTUP_TIMEOUT_SECONDS"),
                loadWorkerScript(WorkerType.LITE_MEMBER, simulatorProperties.get("VENDOR")),
                liteMemberEnv);
    }

    private WorkerParameters loadMemberWorkerParameters(String licenseKey) {
        Map<String, String> memberEnv = new HashMap<String, String>();
        memberEnv.put("AUTOCREATE_HAZELCAST_INSTANCE", "" + options.valueOf(autoCreateHzInstanceSpec));
        memberEnv.put("LOG4j_CONFIG", loadLog4jConfig());
        memberEnv.put("JVM_OPTIONS", options.valueOf(workerVmOptionsSpec));
        memberEnv.put("WORKER_PERFORMANCE_MONITOR_INTERVAL_SECONDS",
                Integer.toString(coordinatorParameters.getPerformanceMonitorIntervalSeconds()));
        memberEnv.put("HAZELCAST_CONFIG",
                initMemberHzConfig(loadMemberHzConfig(), componentRegistry, simulatorProperties.getHazelcastPort(),
                        licenseKey, simulatorProperties, false));

        return new WorkerParameters(
                simulatorProperties.getVersionSpec(),
                simulatorProperties.getAsInteger("WORKER_STARTUP_TIMEOUT_SECONDS"),
                loadWorkerScript(WorkerType.MEMBER, simulatorProperties.get("VENDOR")),
                memberEnv);
    }

    private int getPerformanceMonitorInterval() {
        if (!options.has(monitorPerformanceSpec)) {
            return 0;
        }

        String intervalSeconds = simulatorProperties.get("WORKER_PERFORMANCE_MONITOR_INTERVAL_SECONDS");
        if (intervalSeconds == null || intervalSeconds.isEmpty()) {
            return DEFAULT_WORKER_PERFORMANCE_MONITOR_INTERVAL_SECONDS;
        }
        return Integer.parseInt(intervalSeconds);
    }

    void run() {
        if (options.has(remoteSpec)) {
            coordinator.startRemoteMode();
        } else {
            coordinator.run(deploymentPlan, testSuite);
        }
    }

    private TestSuite loadTestSuite() {
        if (options.hasArgument(remoteSpec)) {
            return null;
        }

        int durationSeconds = getDurationSeconds(durationSpec);
        boolean hasWaitForTestCase = options.has(waitForTestCaseSpec);
        if (!options.has(durationSpec) && hasWaitForTestCase) {
            durationSeconds = 0;
        }

        TestSuite testSuite = TestSuite.loadTestSuite(getTestSuiteFile(), options.valueOf(overridesSpec))
                .setDurationSeconds(durationSeconds)
                .setWarmupDurationSeconds(getDurationSeconds(warmupDurationSpec))
                .setWaitForTestCase(hasWaitForTestCase)
                .setFailFast(options.valueOf(failFastSpec))
                .setVerifyEnabled(options.valueOf(verifyEnabledSpec))
                .setParallel(options.has(parallelSpec))
                .setTargetType(options.valueOf(targetTypeSpec))
                .setTargetCount(options.valueOf(targetCountSpec));

        // if the coordinator is not monitoring performance, we don't care for measuring latencies
        if (!options.has(monitorPerformanceSpec)) {
            for (TestCase testCase : testSuite.getTestCaseList()) {
                testCase.setProperty("measureLatency", "false");
            }
        }

        return testSuite;
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

    private ComponentRegistry newComponentRegistry(SimulatorProperties simulatorProperties) {
        ComponentRegistry componentRegistry;
        if (isLocal(simulatorProperties)) {
            componentRegistry = new ComponentRegistry();
            componentRegistry.addAgent("localhost", "localhost");
        } else {
            componentRegistry = loadComponentRegister(getAgentsFile());
        }
        return componentRegistry;
    }

    private DeploymentPlan newDeploymentPlan() {
        if (options.has(remoteSpec)) {
            return null;
        }

        WorkerType workerType = new WorkerType(options.valueOf(clientTypeSpec));
        if (workerType.isMember()) {
            throw new CommandLineExitException("client workerType can't be [member]");
        }

        return createDeploymentPlan(
                componentRegistry,
                workerParametersMap,
                workerType,
                options.valueOf(memberWorkerCountSpec),
                options.valueOf(clientWorkerCountSpec),
                options.valueOf(dedicatedMemberMachinesSpec));

    }

    private File getTestSuiteFile() {
        File testSuiteFile;

        List testsuiteFiles = options.nonOptionArguments();
        if (testsuiteFiles.size() > 1) {
            throw new CommandLineExitException(format("Too many TestSuite files specified: %s", testsuiteFiles));
        } else if (testsuiteFiles.size() == 1) {
            testSuiteFile = new File((String) testsuiteFiles.get(0));
        } else {
            testSuiteFile = new File("test.properties");
        }

        LOGGER.info("Loading TestSuite file: " + testSuiteFile.getAbsolutePath());
        if (!testSuiteFile.exists()) {
            throw new CommandLineExitException(format("TestSuite file '%s' not found", testSuiteFile));
        }
        return testSuiteFile;
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
        File file = getConfigurationFile("worker-" + vendor + "-" + workerType.name() + ".sh");
        LOGGER.info("Loading Hazelcast worker script: " + file.getAbsolutePath());
        return fileAsText(file);
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

    private static int parseDurationWithoutLastChar(TimeUnit timeUnit, String value) {
        String sub = value.substring(0, value.length() - 1);
        return (int) timeUnit.toSeconds(Integer.parseInt(sub));
    }

    public static void main(String[] args) {
        LOGGER.info("Hazelcast Simulator Coordinator");
        LOGGER.info(format("Version: %s, Commit: %s, Build Time: %s",
                getSimulatorVersion(), getCommitIdAbbrev(), getBuildTime()));
        LOGGER.info(format("SIMULATOR_HOME: %s", getSimulatorHome().getAbsolutePath()));

        try {
            CoordinatorCli cli = new CoordinatorCli(args);
            cli.run();
            LOGGER.info("Complete");
        } catch (Exception e) {
            exitWithError(LOGGER, "Failed to run Coordinator", e);
        }
    }
}
