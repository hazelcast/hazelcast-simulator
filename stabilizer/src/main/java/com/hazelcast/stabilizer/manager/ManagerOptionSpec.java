package com.hazelcast.stabilizer.manager;

import joptsimple.OptionParser;
import joptsimple.OptionSpec;

import java.io.File;

public class ManagerOptionSpec {

    OptionParser parser = new OptionParser();
    OptionSpec cleanGymSpec = parser.accepts("cleanGym",
            "Cleans the gym directory on all coaches");
    OptionSpec<String> durationSpec = parser.accepts("duration",
            "Amount of time to run per exercise. Can be e.g. 10 or 10s, 1m or 2h or 3d.")
            .withRequiredArg().ofType(String.class).defaultsTo("60");
    OptionSpec traineeTrackLoggingSpec = parser.accepts("traineeTrackLogging",
            "If the coach is tracking trainee logging");
    OptionSpec<Integer> traineeCountSpec = parser.accepts("traineeVmCount",
            "Number of trainee VM's per coach")
            .withRequiredArg().ofType(Integer.class).defaultsTo(1);
    OptionSpec<String> traineeClassPathSpec = parser.accepts("traineeClassPath",
            "A file/directory containing the " +
                    "classes/jars/resources that are going to be uploaded to the coaches. " +
                    "Use ';' as separator for multiple entries. Wildcard '*' can also be used.")
            .withRequiredArg().ofType(String.class);
    OptionSpec<Integer> traineeStartupTimeoutSpec = parser.accepts("traineeStartupTimeout",
            "The startup timeout in seconds for a trainee")
            .withRequiredArg().ofType(Integer.class).defaultsTo(60);
    OptionSpec<Boolean> monitorPerformanceSpec = parser.accepts("monitorPerformance",
            "If performance monitoring should be done")
            .withRequiredArg().ofType(Boolean.class).defaultsTo(false);
    OptionSpec<Boolean> verifyEnabledSpec = parser.accepts("verifyEnabled",
            "If exercise should be verified")
            .withRequiredArg().ofType(Boolean.class).defaultsTo(true);
    OptionSpec<Boolean> traineeRefreshSpec = parser.accepts("traineeFresh",
            "If the trainee VM's should be replaced after every workout")
            .withRequiredArg().ofType(Boolean.class).defaultsTo(false);
    OptionSpec<Boolean> failFastSpec = parser.accepts("failFast",
            "It the workout should fail immediately when an exercise from a workout fails instead of continuing ")
            .withRequiredArg().ofType(Boolean.class).defaultsTo(true);
    OptionSpec<String> traineeVmOptionsSpec = parser.accepts("traineeVmOptions",
            "Trainee VM options (quotes can be used)")
            .withRequiredArg().ofType(String.class).defaultsTo("");
    OptionSpec<String> traineeHzFileSpec = parser.accepts("traineeHzFile",
            "The Hazelcast xml configuration file for the trainee")
            .withRequiredArg().ofType(String.class).defaultsTo(getDefaultTraineeHzFile());

    static String getDefaultTraineeHzFile(){
        File file = new File("trainee-hazelcast.xml");
        //if something exists in the current working directory, use that.
        if(file.exists()){
            return file.getAbsolutePath();
        }else{
            return Manager.STABILIZER_HOME + File.separator + "conf" + File.separator + "trainee-hazelcast.xml";
        }
    }

    OptionSpec<String> managerHzFileSpec = parser.accepts(
            "managerHzFile", "The client Hazelcast xml configuration file for the manager")
            .withRequiredArg().ofType(String.class).defaultsTo(
                    getDefaultManagerHzFile());

    static String getDefaultManagerHzFile(){
        File file = new File("manager-hazelcast.xml");
        //if something exists in the current working directory, use that.
        if(file.exists()){
            return file.getAbsolutePath();
        }else{
            return Manager.STABILIZER_HOME + File.separator + "conf" + File.separator + "manager-hazelcast.xml";
        }
    }

    OptionSpec<String> traineeJavaVendorSpec = parser.accepts("traineeJavaVendor", "The Java vendor (e.g. " +
            "openjdk or sun) of the JVM used by the trainee). " +
            "If nothing is specified, the coach is free to pick a vendor.")
            .withRequiredArg().ofType(String.class).defaultsTo("");
    OptionSpec<String> traineeJavaVersionSpec = parser.accepts("traineeJavaVersion", "The Java version (e.g. 1.6) " +
            "of the JVM used by the trainee). " +
            "If nothing is specified, the coach is free to pick a version.")
            .withRequiredArg().ofType(String.class).defaultsTo("");
    OptionSpec<Integer> exerciseStopTimeoutMsSpec = parser.accepts("exerciseStopTimeoutMs", "Maximum amount of time " +
            "waiting for the exercise to stop")
            .withRequiredArg().ofType(Integer.class).defaultsTo(60000);

    OptionSpec helpSpec = parser.accepts("help", "Show help").forHelp();


}
