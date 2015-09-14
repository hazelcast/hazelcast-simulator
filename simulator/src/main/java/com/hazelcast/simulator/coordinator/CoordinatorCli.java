package com.hazelcast.simulator.coordinator;

import com.hazelcast.simulator.agent.workerjvm.WorkerJvmSettings;
import com.hazelcast.simulator.common.AgentsFile;
import com.hazelcast.simulator.test.Failure;
import com.hazelcast.simulator.test.TestPhase;
import com.hazelcast.simulator.test.TestSuite;
import com.hazelcast.simulator.utils.CommandLineExitException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.log4j.Logger;

import java.io.File;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.simulator.common.SimulatorProperties.PROPERTIES_FILE_NAME;
import static com.hazelcast.simulator.test.Failure.Type.fromPropertyValue;
import static com.hazelcast.simulator.test.TestSuite.loadTestSuite;
import static com.hazelcast.simulator.utils.CliUtils.initOptionsWithHelp;
import static com.hazelcast.simulator.utils.FileUtils.fileAsText;
import static com.hazelcast.simulator.utils.FileUtils.getFile;
import static com.hazelcast.simulator.utils.FileUtils.getFileAsTextFromWorkingDirOrBaseDir;
import static com.hazelcast.simulator.utils.FileUtils.newFile;
import static java.lang.Boolean.parseBoolean;
import static java.lang.String.format;

final class CoordinatorCli {

    private static final Logger LOGGER = Logger.getLogger(CoordinatorCli.class);

    private final OptionParser parser = new OptionParser();

    private final OptionSpec<String> durationSpec = parser.accepts("duration",
            "Amount of time to execute run phase per test, e.g. 10s, 1m, 2h or 3d.")
            .withRequiredArg().ofType(String.class);

    private final OptionSpec waitForTestCaseSpec = parser.accepts("waitForTestCaseCompletion",
            "Wait for the TestCase to finish its run phase. Can be combined with --duration to limit runtime.");

    private final OptionSpec<String> overridesSpec = parser.accepts("overrides",
            "Properties that override the properties in a given test-case, e.g. --overrides"
                    + " \"threadcount=20,writeProb=0.2\". This makes it easy to parametrize a test.")
            .withRequiredArg().ofType(String.class).defaultsTo("");

    private final OptionSpec<Integer> memberWorkerCountSpec = parser.accepts("memberWorkerCount",
            "Number of cluster member worker JVMs. If no value is specified and no mixed members are specified,"
                    + " then the number of cluster members will be equal to the number of machines in the agents file.")
            .withRequiredArg().ofType(Integer.class).defaultsTo(-1);

    private final OptionSpec<Integer> clientWorkerCountSpec = parser.accepts("clientWorkerCount",
            "Number of Cluster Client Worker JVMs.")
            .withRequiredArg().ofType(Integer.class).defaultsTo(0);

    private final OptionSpec<Boolean> autoCreateHZInstanceSpec = parser.accepts("autoCreateHzInstances",
            "Auto create Hazelcast instances.")
            .withRequiredArg().ofType(Boolean.class).defaultsTo(true);

    private final OptionSpec<Integer> dedicatedMemberMachinesSpec = parser.accepts("dedicatedMemberMachines",
            "Controls the number of dedicated member machines. For example when there are 4 machines,"
                    + " 2 members and 9 clients with 1 dedicated member machine defined, then"
                    + " 1 machine gets the 2 members and the 3 remaining machines get 3 clients each.")
            .withRequiredArg().ofType(Integer.class);

    private final OptionSpec<String> workerClassPathSpec = parser.accepts("workerClassPath",
            "A file/directory containing the classes/jars/resources that are going to be uploaded to the agents."
                    + " Use ';' as separator for multiple entries. The wildcard '*' can also be used.")
            .withRequiredArg().ofType(String.class);

    private final OptionSpec monitorPerformanceSpec = parser.accepts("monitorPerformance",
            "If defined performance of tests is tracked.");

    private final OptionSpec<Boolean> verifyEnabledSpec = parser.accepts("verifyEnabled",
            "Defines if tests are verified.")
            .withRequiredArg().ofType(Boolean.class).defaultsTo(true);

    private final OptionSpec<Boolean> workerRefreshSpec = parser.accepts("workerFresh",
            "If the worker JVMs should be replaced after every testsuite.")
            .withRequiredArg().ofType(Boolean.class).defaultsTo(false);

    private final OptionSpec<Boolean> failFastSpec = parser.accepts("failFast",
            "Defines if the testsuite should fail immediately when a test from a testsuite fails instead of continuing.")
            .withRequiredArg().ofType(Boolean.class).defaultsTo(true);

    private final OptionSpec<String> tolerableFailureSpec = parser.accepts("tolerableFailure",
            String.format("Defines if tests should not fail when given failure is detected. List of known failures: %s",
                    Failure.Type.getIdsAsString()))
            .withRequiredArg().ofType(String.class);

    private final OptionSpec parallelSpec = parser.accepts("parallel",
            "If defined tests are run in parallel.");

    private final OptionSpec<TestPhase> syncToTestPhaseSpec = parser.accepts("syncToTestPhase",
            "Defines the last TestPhase which is synchronized between all parallel running tests.")
            .withRequiredArg().ofType(TestPhase.class).defaultsTo(TestPhase.SETUP);

    private final OptionSpec<String> workerVmOptionsSpec = parser.accepts("workerVmOptions",
            "Worker JVM options (quotes can be used). These options will be applied to regular members and mixed members"
                    + " (so with client + member in the same JVM).")
            .withRequiredArg().ofType(String.class).defaultsTo("-XX:+HeapDumpOnOutOfMemoryError");

    private final OptionSpec<String> clientWorkerVmOptionsSpec = parser.accepts("clientWorkerVmOptions",
            "Client worker JVM options (quotes can be used).")
            .withRequiredArg().ofType(String.class).defaultsTo("-XX:+HeapDumpOnOutOfMemoryError");

    private final OptionSpec<String> agentsFileSpec = parser.accepts("agentsFile",
            "The file containing the list of agent machines.")
            .withRequiredArg().ofType(String.class).defaultsTo(AgentsFile.NAME);

    private final OptionSpec<String> propertiesFileSpec = parser.accepts("propertiesFile",
            format("The file containing the simulator properties. If no file is explicitly configured,"
                            + " first the working directory is checked for a file '%s'."
                            + " All missing properties are always loaded from SIMULATOR_HOME/conf/%s",
                    PROPERTIES_FILE_NAME, PROPERTIES_FILE_NAME))
            .withRequiredArg().ofType(String.class);

    private final OptionSpec<String> memberHzConfigFileSpec = parser.accepts("hzFile",
            "The Hazelcast XML configuration file for the worker. If no file is explicitly configured,"
                    + " first the 'hazelcast.xml' in the working directory is loaded."
                    + " If that doesn't exist then SIMULATOR_HOME/conf/hazelcast.xml is loaded.")
            .withRequiredArg().ofType(String.class).defaultsTo(getDefaultMemberHzConfigFile());

    private final OptionSpec<String> clientHzConfigFileSpec = parser.accepts("clientHzFile",
            "The client Hazelcast XML configuration file for the worker. If no file is explicitly configured,"
                    + " first the 'client-hazelcast.xml' in the working directory is loaded."
                    + " If that doesn't exist then SIMULATOR_HOME/conf/client-hazelcast.xml is loaded.")
            .withRequiredArg().ofType(String.class).defaultsTo(getDefaultClientHzConfigFile());

    private final OptionSpec<Integer> workerStartupTimeoutSpec = parser.accepts("workerStartupTimeout",
            "The startup timeout in seconds for a worker.")
            .withRequiredArg().ofType(Integer.class).defaultsTo(60);

    private final OptionSpec<Integer> testStopTimeoutMsSpec = parser.accepts("testStopTimeoutMs",
            "Maximum amount of time in milliseconds waiting for the test to stop.")
            .withRequiredArg().ofType(Integer.class).defaultsTo(60000);

    private final Coordinator coordinator;
    private final OptionSet options;

    CoordinatorCli(Coordinator coordinator, String[] args) {
        this.coordinator = coordinator;
        this.options = initOptionsWithHelp(parser, args);
    }

    private static String getDefaultMemberHzConfigFile() {
        File file = new File("hazelcast.xml");
        // if something exists in the current working directory, use that
        if (file.exists()) {
            return file.getAbsolutePath();
        } else {
            return Coordinator.SIMULATOR_HOME + File.separator + "conf" + File.separator + "hazelcast.xml";
        }
    }

    private static String getDefaultClientHzConfigFile() {
        File file = new File("client-hazelcast.xml");
        // if something exists in the current working directory, use that
        if (file.exists()) {
            return file.getAbsolutePath();
        } else {
            return Coordinator.SIMULATOR_HOME + File.separator + "conf" + File.separator + "client-hazelcast.xml";
        }
    }

    void init() {
        if (options.has(workerClassPathSpec)) {
            coordinator.workerClassPath = options.valueOf(workerClassPathSpec);
        }

        coordinator.props.init(getPropertiesFile());
        coordinator.verifyEnabled = options.valueOf(verifyEnabledSpec);
        coordinator.monitorPerformance = options.has(monitorPerformanceSpec);
        coordinator.testStopTimeoutMs = options.valueOf(testStopTimeoutMsSpec);
        coordinator.agentsFile = getFile(agentsFileSpec, options, "Agents file");
        coordinator.parallel = options.has(parallelSpec);
        coordinator.lastTestPhaseToSync = options.valueOf(syncToTestPhaseSpec);

        TestSuite testSuite = loadTestSuite(getTestSuiteFile(), options.valueOf(overridesSpec));
        testSuite.durationSeconds = getDurationSeconds();
        testSuite.waitForTestCase = options.has(waitForTestCaseSpec);
        testSuite.failFast = options.valueOf(failFastSpec);
        testSuite.tolerableFailures = fromPropertyValue(options.valueOf(tolerableFailureSpec));
        coordinator.testSuite = testSuite;

        if (testSuite.durationSeconds == 0 && !testSuite.waitForTestCase) {
            throw new CommandLineExitException("You need to define --duration or --waitForTestCase or both!");
        }

        WorkerJvmSettings workerJvmSettings = new WorkerJvmSettings();
        workerJvmSettings.vmOptions = options.valueOf(workerVmOptionsSpec);
        workerJvmSettings.clientVmOptions = options.valueOf(clientWorkerVmOptionsSpec);
        workerJvmSettings.memberWorkerCount = options.valueOf(memberWorkerCountSpec);
        workerJvmSettings.clientWorkerCount = options.valueOf(clientWorkerCountSpec);
        workerJvmSettings.autoCreateHZInstance = options.valueOf(autoCreateHZInstanceSpec);
        workerJvmSettings.passiveMembers = parseBoolean(coordinator.props.get("PASSIVE_MEMBERS", "true"));

        workerJvmSettings.workerStartupTimeout = options.valueOf(workerStartupTimeoutSpec);
        workerJvmSettings.memberHzConfig = loadMemberHzConfig();
        workerJvmSettings.clientHzConfig = loadClientHzConfig();
        workerJvmSettings.log4jConfig = getFileAsTextFromWorkingDirOrBaseDir(Coordinator.SIMULATOR_HOME,
                "worker-log4j.xml", "Log4j configuration for worker"
        );
        workerJvmSettings.refreshJvm = options.valueOf(workerRefreshSpec);
        workerJvmSettings.profiler = coordinator.props.get("PROFILER", "none");
        workerJvmSettings.yourkitConfig = coordinator.props.get("YOURKIT_SETTINGS");
        workerJvmSettings.flightrecorderSettings = coordinator.props.get("FLIGHTRECORDER_SETTINGS");
        workerJvmSettings.hprofSettings = coordinator.props.get("HPROF_SETTINGS", "");
        workerJvmSettings.perfSettings = coordinator.props.get("PERF_SETTINGS", "");
        workerJvmSettings.vtuneSettings = coordinator.props.get("VTUNE_SETTINGS", "");
        workerJvmSettings.numaCtl = coordinator.props.get("NUMA_CONTROL", "none");

        if (options.has(dedicatedMemberMachinesSpec)) {
            int dedicatedMemberCount = dedicatedMemberMachinesSpec.value(options);
            if (dedicatedMemberCount < 0) {
                throw new CommandLineExitException("dedicatedMemberCount can't be smaller than 0");
            }
            coordinator.dedicatedMemberMachineCount = dedicatedMemberCount;
        }

        coordinator.workerJvmSettings = workerJvmSettings;
    }

    private String loadMemberHzConfig() {
        File file = getFile(memberHzConfigFileSpec, options, "Worker Hazelcast config file");
        LOGGER.info("Loading Hazelcast configuration: " + file.getAbsolutePath());
        return fileAsText(file);
    }

    private String loadClientHzConfig() {
        File file = getFile(clientHzConfigFileSpec, options, "Worker Client Hazelcast config file");
        LOGGER.info("Loading Hazelcast client configuration: " + file.getAbsolutePath());
        return fileAsText(file);
    }

    private File getPropertiesFile() {
        if (options.has(propertiesFileSpec)) {
            // a file was explicitly configured
            return newFile(options.valueOf(propertiesFileSpec));
        } else {
            return null;
        }
    }

    private File getTestSuiteFile() {
        File testSuiteFile;

        List testsuiteFiles = options.nonOptionArguments();
        if (testsuiteFiles.size() > 1) {
            throw new CommandLineExitException(format("Too many testsuite files specified: %s", testsuiteFiles));
        } else if (testsuiteFiles.size() == 1) {
            testSuiteFile = new File((String) testsuiteFiles.get(0));
        } else {
            testSuiteFile = new File("test.properties");
        }

        LOGGER.info("Loading testsuite file: " + testSuiteFile.getAbsolutePath());
        if (!testSuiteFile.exists()) {
            throw new CommandLineExitException(format("TestSuite file '%s' not found", testSuiteFile));
        }
        return testSuiteFile;
    }

    private int getDurationSeconds() {
        if (!options.has(durationSpec)) {
            return 0;
        }

        int duration;
        String value = options.valueOf(durationSpec);
        try {
            if (value.endsWith("s")) {
                String sub = value.substring(0, value.length() - 1);
                duration = Integer.parseInt(sub);
            } else if (value.endsWith("m")) {
                String sub = value.substring(0, value.length() - 1);
                duration = (int) TimeUnit.MINUTES.toSeconds(Integer.parseInt(sub));
            } else if (value.endsWith("h")) {
                String sub = value.substring(0, value.length() - 1);
                duration = (int) TimeUnit.HOURS.toSeconds(Integer.parseInt(sub));
            } else if (value.endsWith("d")) {
                String sub = value.substring(0, value.length() - 1);
                duration = (int) TimeUnit.DAYS.toSeconds(Integer.parseInt(sub));
            } else {
                duration = Integer.parseInt(value);
            }
        } catch (NumberFormatException e) {
            throw new CommandLineExitException(format("Failed to parse duration '%s'", value), e);
        }

        if (duration < 1) {
            throw new CommandLineExitException("duration must be a positive number, but was: " + duration);
        }
        return duration;
    }
}
