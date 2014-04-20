package com.hazelcast.stabilizer.coordinator;

import com.hazelcast.stabilizer.Utils;
import com.hazelcast.stabilizer.agent.WorkerJvmSettings;
import com.hazelcast.stabilizer.tests.Workout;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import java.io.File;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.stabilizer.Utils.asText;
import static com.hazelcast.stabilizer.Utils.exitWithError;
import static com.hazelcast.stabilizer.Utils.getFile;
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

    private final OptionSpec<Integer> workerCountSpec = parser.accepts("workerVmCount",
            "Number of worker JVM's per agent")
            .withRequiredArg().ofType(Integer.class).defaultsTo(1);

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
            "If the worker JVM's should be replaced after every workout")
            .withRequiredArg().ofType(Boolean.class).defaultsTo(false);

    private final OptionSpec<Boolean> failFastSpec = parser.accepts("failFast",
            "It the workout should fail immediately when a Test from a workout fails instead of continuing ")
            .withRequiredArg().ofType(Boolean.class).defaultsTo(true);

    private final OptionSpec<String> workerVmOptionsSpec = parser.accepts("workerVmOptions",
            "Worker VM options (quotes can be used)")
            .withRequiredArg().ofType(String.class).defaultsTo("");

    private final OptionSpec<String> machineListFileSpec = parser.accepts("machineListFile",
            "The file containing the list of agent machines")
            .withRequiredArg().ofType(String.class).defaultsTo("machine_list.txt");

    private final OptionSpec<String> workerHzFileSpec = parser.accepts("workerHzFile",
            "The Hazelcast xml configuration file for the worker")
            .withRequiredArg().ofType(String.class).defaultsTo(getDefaultWorkerHzFile());

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

    private static String getDefaultWorkerHzFile() {
        File file = new File("worker-hazelcast.xml");
        //if something exists in the current working directory, use that.
        if (file.exists()) {
            return file.getAbsolutePath();
        } else {
            return Coordinator.STABILIZER_HOME + File.separator + "conf" + File.separator + "worker-hazelcast.xml";
        }
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

            coordinator.verifyEnabled = options.valueOf(optionSpec.verifyEnabledSpec);
            coordinator.monitorPerformance = options.valueOf(optionSpec.monitorPerformanceSpec);
            coordinator.testStopTimeoutMs = options.valueOf(optionSpec.testStopTimeoutMsSpec);
            coordinator.machineListFile = getFile(optionSpec.machineListFileSpec, options, "Machine list file");

            String workoutFileName = "workout.properties";
            List<String> workoutFiles = options.nonOptionArguments();
            if (workoutFiles.size() == 1) {
                workoutFileName = workoutFiles.get(0);
            } else if (workoutFiles.size() > 1) {
                exitWithError("Too many workout files specified.");
            }

            Workout workout = Workout.createWorkout(new File(workoutFileName));
            coordinator.workout = workout;
            workout.duration = getDuration(optionSpec, options);
            workout.failFast = options.valueOf(optionSpec.failFastSpec);

            WorkerJvmSettings workerJvmSettings = new WorkerJvmSettings();
            workerJvmSettings.trackLogging = options.has(optionSpec.workerTrackLoggingSpec);
            workerJvmSettings.vmOptions = options.valueOf(optionSpec.workerVmOptionsSpec);
            workerJvmSettings.workerCount = options.valueOf(optionSpec.workerCountSpec);
            workerJvmSettings.workerStartupTimeout = options.valueOf(optionSpec.workerStartupTimeoutSpec);
            workerJvmSettings.hzConfig = asText(getFile(optionSpec.workerHzFileSpec, options, "Worker Hazelcast config file"));
            workerJvmSettings.refreshJvm = options.valueOf(optionSpec.workerRefreshSpec);
            workerJvmSettings.javaVendor = options.valueOf(optionSpec.workerJavaVendorSpec);
            workerJvmSettings.javaVersion = options.valueOf(optionSpec.workerJavaVersionSpec);
            workout.workerJvmSettings = workerJvmSettings;
        } catch (OptionException e) {
            Utils.exitWithError(e.getMessage() + ". Use --help to get overview of the help options.");
        }
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
