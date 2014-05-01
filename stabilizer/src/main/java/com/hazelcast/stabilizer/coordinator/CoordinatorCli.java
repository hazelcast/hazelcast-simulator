package com.hazelcast.stabilizer.coordinator;

import com.hazelcast.stabilizer.Utils;
import com.hazelcast.stabilizer.agent.workerjvm.WorkerJvmSettings;
import com.hazelcast.stabilizer.tests.TestSuite;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.stabilizer.Utils.exitWithError;
import static com.hazelcast.stabilizer.Utils.fileAsText;
import static com.hazelcast.stabilizer.Utils.getFile;
import static com.hazelcast.stabilizer.tests.TestSuite.loadTestSuite;
import static java.lang.String.format;

public class CoordinatorCli {

    private final OptionParser parser = new OptionParser();

    private final OptionSpec cleanWorkersHome = parser.accepts("cleanWorkersHome",
            "Cleans the workers home on all agents");

    private final OptionSpec<String> durationSpec = parser.accepts("duration",
            "Amount of time to run per test. Can be e.g. 10 or 10s, 1m or 2h or 3d.")
            .withRequiredArg().ofType(String.class).defaultsTo("60");

    private final OptionSpec workerTrackLoggingSpec = parser.accepts("workerTrackLogging",
            "If the agent is tracking worker logging");

    private final OptionSpec<Integer> memberWorkerCountSpec = parser.accepts("memberWorkerCount",
            "Number of Cluster member Worker JVM's. If no value is specified and no mixed members are specified, " +
                    "then the number of cluster members will be equal to the number of machines in the agents file"
    )
            .withRequiredArg().ofType(Integer.class).defaultsTo(-1);

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

    private final OptionSpec<Integer> workerStartupTimeoutSpec = parser.accepts("workerStartupTimeout",
            "The startup timeout in seconds for a worker")
            .withRequiredArg().ofType(Integer.class).defaultsTo(60);

    private final OptionSpec<Boolean> monitorPerformanceSpec = parser.accepts("monitorPerformance",
            "If performance monitoring should be done")
            .withRequiredArg().ofType(Boolean.class).defaultsTo(false);

    private final OptionSpec<Boolean> verifyEnabledSpec = parser.accepts("verifyEnabled",
            "If test should be verified")
            .withRequiredArg().ofType(Boolean.class).defaultsTo(true);

    private final OptionSpec<Boolean> workerRefreshSpec = parser.accepts("workerFresh",
            "If the worker JVM's should be replaced after every testsuite")
            .withRequiredArg().ofType(Boolean.class).defaultsTo(false);

    private final OptionSpec<Boolean> failFastSpec = parser.accepts("failFast",
            "It the testsuite should fail immediately when a Test from a testsuite fails instead of continuing ")
            .withRequiredArg().ofType(Boolean.class).defaultsTo(true);

    private final OptionSpec<String> workerVmOptionsSpec = parser.accepts("workerVmOptions",
            "Worker VM options (quotes can be used)")
            .withRequiredArg().ofType(String.class).defaultsTo("");

    private final OptionSpec<String> agentsFileSpec = parser.accepts("agentsFile",
            "The file containing the list of agent machines")
            .withRequiredArg().ofType(String.class).defaultsTo("agents.txt");

    private final OptionSpec<String> hzFileSpec = parser.accepts("hzFile",
            "The Hazelcast xml configuration file for the worker")
            .withRequiredArg().ofType(String.class).defaultsTo(getDefaultHzFile());

    private final OptionSpec<String> clientHzFileSpec = parser.accepts("clientHzFile",
            "The client Hazelcast xml configuration file for the worker")
            .withRequiredArg().ofType(String.class).defaultsTo(getDefaultClientHzFile());

    private final OptionSpec<String> workerJavaVendorSpec = parser.accepts("workerJavaVendor",
            "The Java vendor (e.g. openjdk or sun) of the JVM used by the worker). " +
                    "If nothing is specified, the agent is free to pick a vendor."
    )
            .withRequiredArg().ofType(String.class).defaultsTo("");
    private final OptionSpec<String> workerJavaVersionSpec = parser.accepts("workerJavaVersion",
            "The Java version (e.g. 1.6) of the JVM used by the worker). " +
                    "If nothing is specified, the agent is free to pick a version."
    )
            .withRequiredArg().ofType(String.class).defaultsTo("");
    private final OptionSpec<Integer> testStopTimeoutMsSpec = parser.accepts("testStopTimeoutMs",
            "Maximum amount of time waiting for the Test to stop")
            .withRequiredArg().ofType(Integer.class).defaultsTo(60000);

    private final OptionSpec helpSpec = parser.accepts("help", "Show help").forHelp();

    private static String getDefaultHzFile() {
        File file = new File("worker-hazelcast.xml");
        //if something exists in the current working directory, use that.
        if (file.exists()) {
            return file.getAbsolutePath();
        } else {
            return Coordinator.STABILIZER_HOME + File.separator + "conf" + File.separator + "hazelcast.xml";
        }
    }

    private static String getDefaultClientHzFile() {
        File file = new File("worker-client-hazelcast.xml");
        //if something exists in the current working directory, use that.
        if (file.exists()) {
            return file.getAbsolutePath();
        } else {
            return Coordinator.STABILIZER_HOME + File.separator + "conf" + File.separator + "client-hazelcast.xml";
        }
    }

    public static Properties loadStabilizerProperties(String file) {
        Properties properties = new Properties();

        try {
            FileInputStream inputStream = new FileInputStream(file);
            try {
                properties.load(inputStream);
            } catch (IOException e) {
                Utils.closeQuietly(inputStream);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return properties;
    }

    public static void init(Coordinator coordinator, String[] args) throws Exception {
        CoordinatorCli optionSpec = new CoordinatorCli();

        try {
            OptionSet options = optionSpec.parser.parse(args);

            if (options.has(optionSpec.helpSpec)) {
                optionSpec.parser.printHelpOn(System.out);
                System.exit(0);
            }

            coordinator.cleanWorkersHome = options.has(optionSpec.cleanWorkersHome);

            if (options.has(optionSpec.workerClassPathSpec)) {
                coordinator.workerClassPath = options.valueOf(optionSpec.workerClassPathSpec);
            }

            coordinator.properties = loadStabilizerProperties("stabilizer.properties");
            coordinator.verifyEnabled = options.valueOf(optionSpec.verifyEnabledSpec);
            coordinator.monitorPerformance = options.valueOf(optionSpec.monitorPerformanceSpec);
            coordinator.testStopTimeoutMs = options.valueOf(optionSpec.testStopTimeoutMsSpec);
            coordinator.agentsFile = getFile(optionSpec.agentsFileSpec, options, "Agents file");

            TestSuite testSuite = loadTestSuite(getTestSuiteFile(options));
            coordinator.testSuite = testSuite;
            testSuite.duration = getDuration(optionSpec, options);
            testSuite.failFast = options.valueOf(optionSpec.failFastSpec);

            WorkerJvmSettings workerJvmSettings = new WorkerJvmSettings();
            workerJvmSettings.trackLogging = options.has(optionSpec.workerTrackLoggingSpec);
            workerJvmSettings.vmOptions = options.valueOf(optionSpec.workerVmOptionsSpec);
            workerJvmSettings.memberWorkerCount = options.valueOf(optionSpec.memberWorkerCountSpec);
            workerJvmSettings.clientWorkerCount = options.valueOf(optionSpec.clientWorkerCountSpec);
            workerJvmSettings.mixedWorkerCount = options.valueOf(optionSpec.mixedWorkerCountSpec);
            workerJvmSettings.workerStartupTimeout = options.valueOf(optionSpec.workerStartupTimeoutSpec);
            workerJvmSettings.hzConfig = fileAsText(getFile(optionSpec.hzFileSpec, options, "Worker Hazelcast config file"));
            workerJvmSettings.clientHzConfig = fileAsText(getFile(optionSpec.clientHzFileSpec, options, "Worker Client Hazelcast config file"));
            workerJvmSettings.refreshJvm = options.valueOf(optionSpec.workerRefreshSpec);
            workerJvmSettings.javaVendor = options.valueOf(optionSpec.workerJavaVendorSpec);
            workerJvmSettings.javaVersion = options.valueOf(optionSpec.workerJavaVersionSpec);
            String profiler = coordinator.properties.getProperty("PROFILER", "none");
            if (profiler.equals("yourkit")) {
                workerJvmSettings.yourkitConfig = coordinator.properties.getProperty("YOURKIT_SETTINGS");
            }

            coordinator.workerJvmSettings = workerJvmSettings;
        } catch (OptionException e) {
            Utils.exitWithError(e.getMessage() + ". Use --help to get overview of the help options.");
        }
    }

    private static File getTestSuiteFile(OptionSet options) {
        String testsuiteFileName = new File(
                Coordinator.STABILIZER_HOME + Utils.FILE_SEPERATOR + "tests" + Utils.FILE_SEPERATOR,
                "map.properties"
        ).getAbsolutePath();

        List<String> testsuiteFiles = options.nonOptionArguments();
        if (testsuiteFiles.size() == 1) {
            testsuiteFileName = testsuiteFiles.get(0);
        } else if (testsuiteFiles.size() > 1) {
            exitWithError("Too many testsuite files specified.");
        }

        File testSuiteFile = new File(testsuiteFileName);
        if (!testSuiteFile.exists()) {
            Utils.exitWithError(format("Can't find testsuite file [%s]", testSuiteFile));
        }
        return testSuiteFile;
    }

    private static int getDuration(CoordinatorCli optionSpec, OptionSet options) {
        String value = options.valueOf(optionSpec.durationSpec);

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
            exitWithError(format("Failed to parse duration [%s], cause: %s", value, e.getMessage()));
            return -1;
        }
    }
}
