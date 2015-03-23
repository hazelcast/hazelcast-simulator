package com.hazelcast.simulator.coordinator;

import com.hazelcast.simulator.agent.workerjvm.WorkerJvmSettings;
import com.hazelcast.simulator.provisioner.Provisioner;
import com.hazelcast.simulator.test.Failure;
import com.hazelcast.simulator.test.TestSuite;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.log4j.Logger;

import java.io.File;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.simulator.utils.CommonUtils.exitWithError;
import static com.hazelcast.simulator.utils.FileUtils.fileAsText;
import static com.hazelcast.simulator.utils.FileUtils.getFile;
import static com.hazelcast.simulator.utils.FileUtils.getFileAsTextFromWorkingDirOrSimulatorHome;
import static com.hazelcast.simulator.utils.FileUtils.newFile;
import static java.lang.String.format;

public class CoordinatorCli {

    private static final Logger LOGGER = Logger.getLogger(CoordinatorCli.class);

    private final OptionParser parser = new OptionParser();

    private final OptionSpec<String> durationSpec = parser.accepts("duration",
            "Amount of time to run per test. Can be e.g. 10 or 10s, 1m or 2h or 3d.")
            .withRequiredArg().ofType(String.class).defaultsTo("60");

    private final OptionSpec<String> overridesSpec = parser.accepts("overrides",
            "Properties that override the properties in a given test-case. E.g. --overrides "
                    + "\"threadcount=20,writePercentage=20\". This makes it easy to parametrize a test.")
            .withRequiredArg().ofType(String.class).defaultsTo("");

    private final OptionSpec<Integer> memberWorkerCountSpec = parser.accepts("memberWorkerCount",
            "Number of Cluster member Worker JVMs. If no value is specified and no mixed members are specified, "
                    + "then the number of cluster members will be equal to the number of machines in the agents file")
            .withRequiredArg().ofType(Integer.class).defaultsTo(-1);

    private final OptionSpec<Integer> clientWorkerCountSpec = parser.accepts("clientWorkerCount",
            "Number of Cluster Client Worker JVMs")
            .withRequiredArg().ofType(Integer.class).defaultsTo(0);

    private final OptionSpec<Boolean> autoCreateHZInstancesSpec = parser.accepts("autoCreateHzInstances",
            "auto create Hazelcast Instance's default to true")
            .withRequiredArg().ofType(Boolean.class).defaultsTo(true);

    private final OptionSpec<Integer> dedicatedMemberMachinesSpec = parser.accepts("dedicatedMemberMachines",
            "Controls the number of dedicated member machines. For example when there are 4 machines"
                    + "and 2 servers and 9 clients, and there is 1 dedicated member machine, then "
                    + "1 machine gets the 2 members and the 3 remaining machines get 3 clients each.")
            .withRequiredArg().ofType(Integer.class);

    private final OptionSpec<String> workerClassPathSpec = parser.accepts("workerClassPath",
            "A file/directory containing the "
                    + "classes/jars/resources that are going to be uploaded to the agents. "
                    + "Use ';' as separator for multiple entries. Wildcard '*' can also be used.")
            .withRequiredArg().ofType(String.class);

    private final OptionSpec monitorPerformanceSpec = parser.accepts("monitorPerformance",
            "Track performance");

    private final OptionSpec<Boolean> verifyEnabledSpec = parser.accepts("verifyEnabled",
            "If test should be verified")
            .withRequiredArg().ofType(Boolean.class).defaultsTo(true);

    private final OptionSpec<Boolean> workerRefreshSpec = parser.accepts("workerFresh",
            "If the worker JVMs should be replaced after every testsuite")
            .withRequiredArg().ofType(Boolean.class).defaultsTo(false);

    private final OptionSpec<Boolean> failFastSpec = parser.accepts("failFast",
            "It the testsuite should fail immediately when a Test from a testsuite fails instead of continuing ")
            .withRequiredArg().ofType(Boolean.class).defaultsTo(true);

    private final OptionSpec<String> tolerableFailureSpec = parser.accepts("tolerableFailure",
            String.format("It the test should not fail when given failure is detected. List of known failures: '%s'",
                    Failure.Type.getIdsAsString()))
            .withRequiredArg().ofType(String.class);

    private final OptionSpec parallelSpec = parser.accepts("parallel",
            "It tests should be run in parallel.");

    private final OptionSpec<String> workerVmOptionsSpec = parser.accepts("workerVmOptions",
            "Worker VM options (quotes can be used). These options will be applied to regular members and mixed members "
                    + "(so with client + member in the same JVM).")
            .withRequiredArg().ofType(String.class).defaultsTo("-XX:+HeapDumpOnOutOfMemoryError");

    private final OptionSpec<String> clientWorkerVmOptionsSpec = parser.accepts("clientWorkerVmOptions",
            "Client worker VM options (quotes can be used).")
            .withRequiredArg().ofType(String.class).defaultsTo("-XX:+HeapDumpOnOutOfMemoryError");

    private final OptionSpec<String> agentsFileSpec = parser.accepts("agentsFile",
            "The file containing the list of agent machines")
            .withRequiredArg().ofType(String.class).defaultsTo(Provisioner.AGENTS_FILE);

    private final OptionSpec<String> propertiesFileSpec = parser.accepts("propertiesFile",
            "The file containing the simulator properties. If no file is explicitly configured, first the "
                    + "working directory is checked for a file 'simulator.properties'. All missing properties"
                    + "are always loaded from SIMULATOR_HOME/conf/simulator.properties")
            .withRequiredArg().ofType(String.class);

    private final OptionSpec<String> hzFileSpec = parser.accepts("hzFile",
            "The Hazelcast xml configuration file for the worker. If one is not explicitly configured, first"
                    + "the 'hazelcast.xml' in the working directory is loaded, if that doesn't exist then "
                    + "SIMULATOR_HOME/conf/hazelcast.xml is loaded.")
            .withRequiredArg().ofType(String.class).defaultsTo(getDefaultHzFile());

    private final OptionSpec<String> clientHzFileSpec = parser.accepts("clientHzFile",
            "The client Hazelcast xml configuration file for the worker. If one is not explicitly configured, first"
                    + "the 'client-hazelcast.xml' in the working directory is loaded, if that doesn't exist then "
                    + "SIMULATOR_HOME/conf/client-hazelcast.xml is loaded.")
            .withRequiredArg().ofType(String.class).defaultsTo(getDefaultClientHzFile());

    private final OptionSpec<Integer> workerStartupTimeoutSpec = parser.accepts("workerStartupTimeout",
            "The startup timeout in seconds for a worker")
            .withRequiredArg().ofType(Integer.class).defaultsTo(60);

    private final OptionSpec<Integer> testStopTimeoutMsSpec = parser.accepts("testStopTimeoutMs",
            "Maximum amount of time waiting for the Test to stop")
            .withRequiredArg().ofType(Integer.class).defaultsTo(60000);

    private final OptionSpec helpSpec = parser.accepts("help", "Show help").forHelp();
    private final Coordinator coordinator;
    private OptionSet options;

    private static String getDefaultHzFile() {
        File file = new File("hazelcast.xml");
        // if something exists in the current working directory, use that.
        if (file.exists()) {
            return file.getAbsolutePath();
        } else {
            return Coordinator.SIMULATOR_HOME + File.separator + "conf" + File.separator + "hazelcast.xml";
        }
    }

    private static String getDefaultClientHzFile() {
        File file = new File("client-hazelcast.xml");
        // if something exists in the current working directory, use that.
        if (file.exists()) {
            return file.getAbsolutePath();
        } else {
            return Coordinator.SIMULATOR_HOME + File.separator + "conf" + File.separator + "client-hazelcast.xml";
        }
    }

    public CoordinatorCli(Coordinator coordinator) {
        this.coordinator = coordinator;
    }

    public void init(String[] args) throws Exception {
        try {
            options = parser.parse(args);
        } catch (OptionException e) {
            exitWithError(LOGGER, e.getMessage() + ". Use --help to get overview of the help options.");
            return;
        }

        if (options.has(helpSpec)) {
            parser.printHelpOn(System.out);
            System.exit(0);
        }

        if (options.has(workerClassPathSpec)) {
            coordinator.workerClassPath = options.valueOf(workerClassPathSpec);
        }

        coordinator.props.init(getPropertiesFile());
        coordinator.verifyEnabled = options.valueOf(verifyEnabledSpec);
        coordinator.monitorPerformance = options.has(monitorPerformanceSpec);
        coordinator.testStopTimeoutMs = options.valueOf(testStopTimeoutMsSpec);
        coordinator.agentsFile = getFile(agentsFileSpec, options, "Agents file");
        coordinator.parallel = options.has(parallelSpec);

        TestSuite testSuite = TestSuite.loadTestSuite(getTestSuiteFile(), options.valueOf(overridesSpec));
        testSuite.duration = getDuration();
        testSuite.failFast = options.valueOf(failFastSpec);
        testSuite.tolerableFailures = Failure.Type.fromPropertyValue(options.valueOf(tolerableFailureSpec));
        coordinator.testSuite = testSuite;

        WorkerJvmSettings workerJvmSettings = new WorkerJvmSettings();
        workerJvmSettings.vmOptions = options.valueOf(workerVmOptionsSpec);
        workerJvmSettings.clientVmOptions = options.valueOf(clientWorkerVmOptionsSpec);
        workerJvmSettings.memberWorkerCount = options.valueOf(memberWorkerCountSpec);
        workerJvmSettings.clientWorkerCount = options.valueOf(clientWorkerCountSpec);
        workerJvmSettings.autoCreateHZInstances = options.valueOf(autoCreateHZInstancesSpec);

        workerJvmSettings.workerStartupTimeout = options.valueOf(workerStartupTimeoutSpec);
        workerJvmSettings.hzConfig = loadHzConfig();
        workerJvmSettings.clientHzConfig = loadClientHzConfig();
        workerJvmSettings.log4jConfig = getFileAsTextFromWorkingDirOrSimulatorHome(
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
                exitWithError(LOGGER, "dedicatedMemberCount can't be smaller than 0");
            }
            coordinator.dedicatedMemberMachineCount = dedicatedMemberCount;
        }

        coordinator.workerJvmSettings = workerJvmSettings;
    }

    @SuppressWarnings("unused")
    private String getProperties() {
        return options.valueOf(propertiesFileSpec);
    }

    private String loadClientHzConfig() {
        File file = getFile(clientHzFileSpec, options, "Worker Client Hazelcast config file");
        LOGGER.info("Loading Hazelcast client configuration: " + file.getAbsolutePath());
        return fileAsText(file);
    }

    private String loadHzConfig() {
        File file = getFile(hzFileSpec, options, "Worker Hazelcast config file");
        LOGGER.info("Loading Hazelcast configuration: " + file.getAbsolutePath());
        return fileAsText(file);
    }

    private File getPropertiesFile() {
        if (options.has(propertiesFileSpec)) {
            //a file was explicitly configured
            return newFile(options.valueOf(propertiesFileSpec));
        } else {
            return null;
        }
    }

    private File getTestSuiteFile() {
        String testsuiteFileName = null;

        List<String> testsuiteFiles = options.nonOptionArguments();
        if (testsuiteFiles.isEmpty()) {
            testsuiteFileName = new File("test.properties").getAbsolutePath();
        } else if (testsuiteFiles.size() == 1) {
            testsuiteFileName = testsuiteFiles.get(0);
        } else if (testsuiteFiles.size() > 1) {
            exitWithError(LOGGER, "Too many testsuite files specified.");
            //won't be executed.
            return null;
        }
        if (testsuiteFileName == null) {
            exitWithError(LOGGER, "TestSuite filename was null.");
            return null;
        }

        File testSuiteFile = new File(testsuiteFileName);
        LOGGER.info("Loading testsuite file: " + testSuiteFile.getAbsolutePath());
        if (!testSuiteFile.exists()) {
            exitWithError(LOGGER, format("Can't find testsuite file [%s]", testSuiteFile));
        }
        return testSuiteFile;
    }

    private int getDuration() {
        String value = options.valueOf(durationSpec);

        try {
            if (value.endsWith("s")) {
                String sub = value.substring(0, value.length() - 1);
                return Integer.parseInt(sub);
            } else if (value.endsWith("m")) {
                String sub = value.substring(0, value.length() - 1);
                return (int) TimeUnit.MINUTES.toSeconds(Integer.parseInt(sub));
            } else if (value.endsWith("h")) {
                String sub = value.substring(0, value.length() - 1);
                return (int) TimeUnit.HOURS.toSeconds(Integer.parseInt(sub));
            } else if (value.endsWith("d")) {
                String sub = value.substring(0, value.length() - 1);
                return (int) TimeUnit.DAYS.toSeconds(Integer.parseInt(sub));
            } else {
                return Integer.parseInt(value);
            }
        } catch (NumberFormatException e) {
            exitWithError(LOGGER, format("Failed to parse duration [%s], cause: %s", value, e.getMessage()));
            return -1;
        }
    }
}
