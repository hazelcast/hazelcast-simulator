package com.hazelcast.stabilizer.console;

import joptsimple.OptionParser;
import joptsimple.OptionSpec;

import java.io.File;

public class ConsoleOptionSpec {

    OptionParser parser = new OptionParser();
    OptionSpec cleanWorkersHome = parser.accepts("cleanWorkersHome",
            "Cleans the workers home on all agents");
    OptionSpec<String> durationSpec = parser.accepts("duration",
            "Amount of time to run per test. Can be e.g. 10 or 10s, 1m or 2h or 3d.")
            .withRequiredArg().ofType(String.class).defaultsTo("60");
    OptionSpec workerTrackLoggingSpec = parser.accepts("workerTrackLogging",
            "If the agent is tracking worker logging");
    OptionSpec<Integer> workerCountSpec = parser.accepts("workerVmCount",
            "Number of worker JVM's per agent")
            .withRequiredArg().ofType(Integer.class).defaultsTo(1);
    OptionSpec<String> workerClassPathSpec = parser.accepts("workerClassPath",
            "A file/directory containing the " +
                    "classes/jars/resources that are going to be uploaded to the agents. " +
                    "Use ';' as separator for multiple entries. Wildcard '*' can also be used.")
            .withRequiredArg().ofType(String.class);
    OptionSpec<Integer> workerStartupTimeoutSpec = parser.accepts("workerStartupTimeout",
            "The startup timeout in seconds for a worker")
            .withRequiredArg().ofType(Integer.class).defaultsTo(60);
    OptionSpec<Boolean> monitorPerformanceSpec = parser.accepts("monitorPerformance",
            "If performance monitoring should be done")
            .withRequiredArg().ofType(Boolean.class).defaultsTo(false);
    OptionSpec<Boolean> verifyEnabledSpec = parser.accepts("verifyEnabled",
            "If test should be verified")
            .withRequiredArg().ofType(Boolean.class).defaultsTo(true);
    OptionSpec<Boolean> workerRefreshSpec = parser.accepts("workerFresh",
            "If the worker JVM's should be replaced after every workout")
            .withRequiredArg().ofType(Boolean.class).defaultsTo(false);
    OptionSpec<Boolean> failFastSpec = parser.accepts("failFast",
            "It the workout should fail immediately when a Test from a workout fails instead of continuing ")
            .withRequiredArg().ofType(Boolean.class).defaultsTo(true);
    OptionSpec<String> workerVmOptionsSpec = parser.accepts("workerVmOptions",
            "Worker VM options (quotes can be used)")
            .withRequiredArg().ofType(String.class).defaultsTo("");
    OptionSpec<String> workerHzFileSpec = parser.accepts("workerHzFile",
            "The Hazelcast xml configuration file for the worker")
            .withRequiredArg().ofType(String.class).defaultsTo(getDefaultWorkerHzFile());

    static String getDefaultWorkerHzFile(){
        File file = new File("worker-hazelcast.xml");
        //if something exists in the current working directory, use that.
        if(file.exists()){
            return file.getAbsolutePath();
        }else{
            return Console.STABILIZER_HOME + File.separator + "conf" + File.separator + "worker-hazelcast.xml";
        }
    }

    OptionSpec<String> consoleHzFileSpec = parser.accepts(
            "consoleHzFile", "The client Hazelcast xml configuration file for the console")
            .withRequiredArg().ofType(String.class).defaultsTo(
                    getDefaultConsoleHzFile());

    static String getDefaultConsoleHzFile(){
        File file = new File("console-hazelcast.xml");
        //if something exists in the current working directory, use that.
        if(file.exists()){
            return file.getAbsolutePath();
        }else{
            return Console.STABILIZER_HOME + File.separator + "conf" + File.separator + "console-hazelcast.xml";
        }
    }

    OptionSpec<String> workerJavaVendorSpec = parser.accepts("workerJavaVendor", "The Java vendor (e.g. " +
            "openjdk or sun) of the JVM used by the worker). " +
            "If nothing is specified, the agent is free to pick a vendor.")
            .withRequiredArg().ofType(String.class).defaultsTo("");
    OptionSpec<String> workerJavaVersionSpec = parser.accepts("workerJavaVersion", "The Java version (e.g. 1.6) " +
            "of the JVM used by the worker). " +
            "If nothing is specified, the agent is free to pick a version.")
            .withRequiredArg().ofType(String.class).defaultsTo("");
    OptionSpec<Integer> testStopTimeoutMsSpec = parser.accepts("testStopTimeoutMs", "Maximum amount of time " +
            "waiting for the Test to stop")
            .withRequiredArg().ofType(Integer.class).defaultsTo(60000);

    OptionSpec helpSpec = parser.accepts("help", "Show help").forHelp();


}
