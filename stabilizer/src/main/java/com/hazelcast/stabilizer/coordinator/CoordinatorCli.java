package com.hazelcast.stabilizer.coordinator;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.Utils;
import com.hazelcast.stabilizer.agent.workerjvm.WorkerJvmSettings;
import com.hazelcast.stabilizer.tests.Failure;
import com.hazelcast.stabilizer.tests.TestSuite;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import java.io.File;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.stabilizer.Utils.exitWithError;
import static com.hazelcast.stabilizer.Utils.fileAsText;
import static com.hazelcast.stabilizer.Utils.getFile;
import static com.hazelcast.stabilizer.Utils.newFile;
import static com.hazelcast.stabilizer.tests.TestSuite.loadTestSuite;
import static java.lang.String.format;

public class CoordinatorCli {

    private final static ILogger log = Logger.getLogger(CoordinatorCli.class);

    private final OptionParser parser = new OptionParser();

    private final OptionSpec<String> durationSpec = parser.accepts("duration",
            "Amount of time to run per test. Can be e.g. 10 or 10s, 1m or 2h or 3d.")
            .withRequiredArg().ofType(String.class).defaultsTo("60");

    private final OptionSpec<Integer> memberWorkerCountSpec = parser.accepts("memberWorkerCount",
            "Number of Cluster member Worker JVM's. If no value is specified and no mixed members are specified, " +
                    "then the number of cluster members will be equal to the number of machines in the agents file"
    ).withRequiredArg().ofType(Integer.class).defaultsTo(-1);

    private final OptionSpec<Integer> clientWorkerCountSpec = parser.accepts("clientWorkerCount",
            "Number of Cluster Client Worker JVM's")
            .withRequiredArg().ofType(Integer.class).defaultsTo(0);

    private final OptionSpec<Integer> mixedWorkerCountSpec = parser.accepts("mixedWorkerCount",
            "Number of Mixed Cluster member JVM's (a client hz + member hz and all communication through client)")
            .withRequiredArg().ofType(Integer.class).defaultsTo(0);

    private final OptionSpec<String> workerClassPathSpec = parser.accepts("workerClassPath",
            "A file/directory containing the " +
                    "classes/jars/resources that are going to be uploaded to the agents. " +
                    "Use ';' as separator for multiple entries. Wildcard '*' can also be used."
    ).withRequiredArg().ofType(String.class);

    private final OptionSpec monitorPerformanceSpec = parser.accepts("monitorPerformance",
            "Track performance");

    private final OptionSpec<Boolean> verifyEnabledSpec = parser.accepts("verifyEnabled",
            "If test should be verified")
            .withRequiredArg().ofType(Boolean.class).defaultsTo(true);

    private final OptionSpec<Boolean> workerRefreshSpec = parser.accepts("workerFresh",
            "If the worker JVM's should be replaced after every testsuite")
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
            "Worker VM options (quotes can be used)")
            .withRequiredArg().ofType(String.class).defaultsTo("-XX:+HeapDumpOnOutOfMemoryError");

    private final OptionSpec<String> agentsFileSpec = parser.accepts("agentsFile",
            "The file containing the list of agent machines")
            .withRequiredArg().ofType(String.class).defaultsTo("agents.txt");

    private final OptionSpec<String> propertiesFileSpec = parser.accepts("propertiesFile",
            "The file containing the stabilizer properties. If no file is explicitly configured, first the " +
                    "working directory is checked for a file 'stabilizer.properties'. All missing properties" +
                    "are always loaded from STABILIZER_HOME/conf/stabilizer.properties"
    ).withRequiredArg().ofType(String.class);

    private final OptionSpec<String> hzFileSpec = parser.accepts("hzFile",
            "The Hazelcast xml configuration file for the worker. If one is not explicitly configured, first" +
                    "the 'hazelcast.xml' in the working directory is loaded, if that doesn't exist then " +
                    "STABILIZER_HOME/conf/hazelcast.xml is loaded."
    ).withRequiredArg().ofType(String.class).defaultsTo(getDefaultHzFile());

    private final OptionSpec<String> clientHzFileSpec = parser.accepts("clientHzFile",
            "The client Hazelcast xml configuration file for the worker. If one is not explicitly configured, first" +
                    "the 'client-hazelcast.xml' in the working directory is loaded, if that doesn't exist then " +
                    "STABILIZER_HOME/conf/client-hazelcast.xml is loaded."
    ).withRequiredArg().ofType(String.class).defaultsTo(getDefaultClientHzFile());

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
        //if something exists in the current working directory, use that.
        if (file.exists()) {
            return file.getAbsolutePath();
        } else {
            return Coordinator.STABILIZER_HOME + File.separator + "conf" + File.separator + "hazelcast.xml";
        }
    }

    private static String getDefaultClientHzFile() {
        File file = new File("client-hazelcast.xml");
        //if something exists in the current working directory, use that.
        if (file.exists()) {
            return file.getAbsolutePath();
        } else {
            return Coordinator.STABILIZER_HOME + File.separator + "conf" + File.separator + "client-hazelcast.xml";
        }
    }

    public CoordinatorCli(Coordinator coordinator) {
        this.coordinator = coordinator;
    }

    public void init(String[] args) throws Exception {
        try {
            options = parser.parse(args);
        } catch (OptionException e) {
            Utils.exitWithError(log, e.getMessage() + ". Use --help to get overview of the help options.");
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

        TestSuite testSuite = loadTestSuite(getTestSuiteFile());
        coordinator.testSuite = testSuite;
        testSuite.duration = getDuration();
        testSuite.failFast = options.valueOf(failFastSpec);
        testSuite.tolerableFailures = Failure.Type.fromPropertyValue(options.valueOf(tolerableFailureSpec));

        WorkerJvmSettings workerJvmSettings = new WorkerJvmSettings();
        workerJvmSettings.vmOptions = options.valueOf(workerVmOptionsSpec);
        workerJvmSettings.memberWorkerCount = options.valueOf(memberWorkerCountSpec);
        workerJvmSettings.clientWorkerCount = options.valueOf(clientWorkerCountSpec);
        workerJvmSettings.mixedWorkerCount = options.valueOf(mixedWorkerCountSpec);
        workerJvmSettings.workerStartupTimeout = options.valueOf(workerStartupTimeoutSpec);
        workerJvmSettings.hzConfig = loadHzConfig();
        workerJvmSettings.clientHzConfig = loadClientHzConfig();
        workerJvmSettings.refreshJvm = options.valueOf(workerRefreshSpec);

        workerJvmSettings.profiler = coordinator.props.get("PROFILER", "none");
        workerJvmSettings.yourkitConfig = coordinator.props.get("YOURKIT_SETTINGS");
        workerJvmSettings.hprofSettings = coordinator.props.get("HPROF_SETTINGS", "");

        coordinator.workerJvmSettings = workerJvmSettings;
    }

    private String loadClientHzConfig() {
        File file = getFile(clientHzFileSpec, options, "Worker Client Hazelcast config file");
        log.info("Loading Hazelcast client configuration: " + file.getAbsolutePath());
        return fileAsText(file);
    }

    private String loadHzConfig() {
        File file = getFile(hzFileSpec, options, "Worker Hazelcast config file");
        log.info("Loading Hazelcast configuration: " + file.getAbsolutePath());
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
            exitWithError(log, "Too many testsuite files specified.");
            //won't be executed.
            return null;
        }

        File testSuiteFile = new File(testsuiteFileName);
        log.info("Loading testsuite file: " + testSuiteFile.getAbsolutePath());
        if (!testSuiteFile.exists()) {
            Utils.exitWithError(log, format("Can't find testsuite file [%s]", testSuiteFile));
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
            exitWithError(log, format("Failed to parse duration [%s], cause: %s", value, e.getMessage()));
            return -1;
        }
    }
}
