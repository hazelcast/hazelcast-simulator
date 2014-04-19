/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.stabilizer.console;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.XmlClientConfigBuilder;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.Member;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.Failure;
import com.hazelcast.stabilizer.TestRecipe;
import com.hazelcast.stabilizer.Utils;
import com.hazelcast.stabilizer.agent.Agent;
import com.hazelcast.stabilizer.performance.Performance;
import com.hazelcast.stabilizer.tests.Workout;
import com.hazelcast.stabilizer.worker.WorkerVmSettings;
import joptsimple.OptionException;
import joptsimple.OptionSet;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.stabilizer.Utils.exitWithError;
import static com.hazelcast.stabilizer.Utils.getStablizerHome;
import static com.hazelcast.stabilizer.Utils.getVersion;
import static com.hazelcast.stabilizer.Utils.secondsToHuman;
import static java.lang.String.format;
import static java.util.Collections.synchronizedList;

public class Console {

    public final static File STABILIZER_HOME = getStablizerHome();
    private final static ILogger log = Logger.getLogger(Console.class);

    private Workout workout;
    private File consoleHzFile;
    private final List<Failure> failureList = synchronizedList(new LinkedList<Failure>());
    private HazelcastInstance client;
    ITopic statusTopic;
    private volatile TestRecipe testRecipe;
    private String workerClassPath;
    private boolean cleanWorkersHome;
    private boolean monitorPerformance;
    private boolean verifyEnabled = true;
    private Integer testStopTimeoutMs;
    private AgentClientManager agentClientManager = new AgentClientManager(this);

    public void setWorkout(Workout workout) {
        this.workout = workout;
    }

    public TestRecipe getTestRecipe() {
        return testRecipe;
    }

    public void setWorkerClassPath(String workerClassPath) {
        this.workerClassPath = workerClassPath;
    }

    public void setCleanWorkersHome(boolean cleanWorkersHome) {
        this.cleanWorkersHome = cleanWorkersHome;
    }

    private void run() throws Exception {
        initClient();

        if (cleanWorkersHome) {
            sendStatusUpdate("Starting cleanup workers home");
            agentClientManager.cleanWorkersHome();
            sendStatusUpdate("Finished cleanup workers home");
        }

        byte[] bytes = createUpload();
        agentClientManager.initWorkout(workout, bytes);

        WorkerVmSettings workerVmSettings = workout.getWorkerVmSettings();
        Set<Member> members = client.getCluster().getMembers();
        log.info(format("Worker track logging: %s", workerVmSettings.isTrackLogging()));
        log.info(format("Workers per agent: %s", workerVmSettings.getWorkerCount()));
        log.info(format("Total number of agents: %s", members.size()));
        log.info(format("Total number of workers: %s", members.size() * workerVmSettings.getWorkerCount()));

        ITopic failureTopic = client.getTopic(Agent.AGENT_STABILIZER_TOPIC);
        failureTopic.addMessageListener(new MessageListener() {
            @Override
            public void onMessage(Message message) {
                Object messageObject = message.getMessageObject();
                if (messageObject instanceof Failure) {
                    Failure failure = (Failure) messageObject;
                    failureList.add(failure);
                    log.severe("Remote failure detected:" + failure);
                } else if (messageObject instanceof Exception) {
                    Exception e = (Exception) messageObject;
                    log.severe(e);
                } else {
                    log.info(messageObject.toString());
                }
            }
        });

        long startMs = System.currentTimeMillis();

        runWorkout(workout);

        //the console needs to sleep some to make sure that it will get failures if they are there.
        log.info("Starting cooldown (10 sec)");
        Utils.sleepSeconds(10);
        log.info("Finished cooldown");

        client.getLifecycleService().shutdown();

        long elapsedMs = System.currentTimeMillis() - startMs;
        log.info(format("Total running time: %s seconds", elapsedMs / 1000));

        if (failureList.isEmpty()) {
            log.info("-----------------------------------------------------------------------------");
            log.info("No failures have been detected!");
            log.info("-----------------------------------------------------------------------------");
            System.exit(0);
        } else {
            StringBuilder sb = new StringBuilder();
            sb.append(failureList.size()).append(" Failures have been detected!!!\n");
            for (Failure failure : failureList) {
                sb.append("-----------------------------------------------------------------------------\n");
                sb.append(failure).append('\n');
            }
            sb.append("-----------------------------------------------------------------------------\n");
            log.severe(sb.toString());
            System.exit(1);
        }
    }

    private byte[] createUpload() throws IOException {
        if (workerClassPath == null)
            return null;

        String[] parts = workerClassPath.split(";");
        List<File> files = new LinkedList<File>();
        for (String filePath : parts) {
            File file = new File(filePath);

            if (file.getName().contains("*")) {
                File parent = file.getParentFile();
                if (!parent.isDirectory()) {
                    throw new IOException(format("Can't create upload, file [%s] is not a directory", parent));
                }

                String regex = file.getName().replace("*", "(.*)");
                for (File child : parent.listFiles()) {
                    if (child.getName().matches(regex)) {
                        files.add(child);
                    }
                }
            } else if (file.exists()) {
                files.add(file);
            } else {
                throw new IOException(format("Can't create upload, file [%s] doesn't exist", filePath));
            }
        }

        return Utils.zip(files);
    }

    private void runWorkout(Workout workout) throws Exception {
        sendStatusUpdate(format("Starting workout: %s", workout.getId()));
        sendStatusUpdate(format("Tests in workout: %s", workout.size()));
        sendStatusUpdate(format("Running time per test: %s ", secondsToHuman(workout.getDuration())));
        sendStatusUpdate(format("Expected total workout time: %s", secondsToHuman(workout.size() * workout.getDuration())));

        //we need to make sure that before we start, there are no workers running anymore.
        //log.log(Level.INFO, "Ensuring workers all killed");
        terminateWorkers();
        startWorkers(workout.getWorkerVmSettings());

        for (TestRecipe testRecipe : workout.getTestRecipeList()) {
            boolean success = run(workout, testRecipe);
            if (!success && workout.isFailFast()) {
                log.info("Aborting working due to failure");
                break;
            }

            if (!success || workout.getWorkerVmSettings().isRefreshJvm()) {
                terminateWorkers();
                startWorkers(workout.getWorkerVmSettings());
            }
        }

        terminateWorkers();
    }

    private boolean run(Workout workout, TestRecipe testRecipe) {
        sendStatusUpdate(format("Running Test : %s", testRecipe.getTestId()));

        this.testRecipe = testRecipe;
        int oldCount = failureList.size();
        try {
            sendStatusUpdate(testRecipe.toString());

            sendStatusUpdate("Starting Test initialization");
            agentClientManager.prepareAgentsForTests(testRecipe);
            agentClientManager.initTest(testRecipe);
            sendStatusUpdate("Completed Test initialization");

            sendStatusUpdate("Starting Test local setup");
            agentClientManager.globalGenericTestTask("localSetup");
            sendStatusUpdate("Completed Test local setup");

            sendStatusUpdate("Starting Test global setup");
            agentClientManager.singleGenericTestTask("globalSet");
            sendStatusUpdate("Completed Test global setup");

            sendStatusUpdate("Starting Test start");
            agentClientManager.globalGenericTestTask("start");
            sendStatusUpdate("Completed Test start");

            sendStatusUpdate(format("Test running for %s seconds", workout.getDuration()));
            sleepSeconds(workout.getDuration());
            sendStatusUpdate("Test finished running");

            sendStatusUpdate("Starting Test stop");
            agentClientManager.stopTest();
            sendStatusUpdate("Completed Test stop");

            if (monitorPerformance) {
                sendStatusUpdate(calcPerformance().toHumanString());
            }

            if (verifyEnabled) {
                sendStatusUpdate("Starting Test global verify");
                agentClientManager.singleGenericTestTask("globalVerify");
                sendStatusUpdate("Completed Test global verify");

                sendStatusUpdate("Starting Test local verify");
                agentClientManager.globalGenericTestTask("localVerify");
                sendStatusUpdate("Completed Test local verify");
            } else {
                sendStatusUpdate("Skipping Test verification");
            }

            sendStatusUpdate("Starting Test global teardown");
            agentClientManager.singleGenericTestTask("globalTearDown");
            sendStatusUpdate("Finished Test global teardown");

            sendStatusUpdate("Starting Test local teardown");
            agentClientManager.globalGenericTestTask("localTearDown");

            sendStatusUpdate("Completed Test local teardown");

            return failureList.size() == oldCount;
        } catch (Exception e) {
            log.severe("Failed", e);
            return false;
        }
    }

      public void sleepSeconds(int seconds) {
        int period = 30;
        int big = seconds / period;
        int small = seconds % period;

        for (int k = 1; k <= big; k++) {
            if (failureList.size() > 0) {
                sendStatusUpdate("Failure detected, aborting execution of test");
                return;
            }

            Utils.sleepSeconds(period);
            final int elapsed = period * k;
            final float percentage = (100f * elapsed) / seconds;
            String msg = format("Running %s of %s seconds %-4.2f percent complete", elapsed, seconds, percentage);
            sendStatusUpdate(msg);
            if (monitorPerformance) {
                sendStatusUpdate(calcPerformance().toHumanString());
            }
        }

        Utils.sleepSeconds(small);
    }

    public Performance calcPerformance() {
        return null;
//        ShoutToWorkersTask task = new ShoutToWorkersTask(new GenericTestTask("calcPerformance"), "calcPerformance");
//        Map<Member, Future<List<Performance>>> result = agentExecutor.submitToAllMembers(task);
//        Performance performance = null;
//        for (Future<List<Performance>> future : result.values()) {
//            try {
//                List<Performance> results = future.get();
//                for (Performance p : results) {
//                    if (performance == null) {
//                        performance = p;
//                    } else {
//                        performance = performance.merge(p);
//                    }
//                }
//            } catch (InterruptedException e) {
//            } catch (ExecutionException e) {
//                log.severe(e);
//            }
//        }
//        return performance == null ? new NotAvailable() : performance;
    }

    private void terminateWorkers() throws Exception {
        sendStatusUpdate("Stopping workers");
        agentClientManager.terminateWorkers();
        sendStatusUpdate("All workers have been terminated");
    }

    private long startWorkers(WorkerVmSettings workerVmSettings) throws Exception {
        long startMs = System.currentTimeMillis();
        final int workerCount = workerVmSettings.getWorkerCount();
        final int totalWorkerCount = workerCount * client.getCluster().getMembers().size();
        log.info(format("Starting a grand total of %s Worker Java Virtual Machines", totalWorkerCount));
        agentClientManager.spawnWorkers(workerVmSettings);
        long durationMs = System.currentTimeMillis() - startMs;
        log.info((format("Finished starting a grand total of %s Workers after %s ms\n", totalWorkerCount, durationMs)));
        return startMs;
    }

    private void sendStatusUpdate(String s) {
        try {
            statusTopic.publish(s);
        } catch (Exception e) {
            log.severe("Failed to echo to all members", e);
        }
    }

//    private void submitToOneWorker(Callable task) throws InterruptedException, ExecutionException {
//        Future future = agentExecutor.submit(new TellWorker(task));
//        try {
//            Object o = future.get(1000, TimeUnit.SECONDS);
//        } catch (ExecutionException e) {
//            if (!(e.getCause() instanceof FailureAlreadyThrownRuntimeException)) {
//                statusTopic.publish(new Failure(null, null, null, null, getTestRecipe(), e));
//            }
//            throw new RuntimeException(e);
//        } catch (TimeoutException e) {
//            Failure failure = new Failure("Timeout waiting for remote operation to complete",
//                    null, null, null, getTestRecipe(), e);
//            statusTopic.publish(failure);
//            throw new RuntimeException(e);
//        }
//    }
//
//    private void submitToAllWorkersAndWait(Callable task, String taskDescription) throws InterruptedException, ExecutionException {
//        ShoutToWorkersTask task1 = new ShoutToWorkersTask(task, taskDescription);
//        submitToAllAgentsAndWait(task1);
//    }

    private void initClient() throws FileNotFoundException {
        ClientConfig clientConfig = new XmlClientConfigBuilder(new FileInputStream(consoleHzFile)).build();
        client = HazelcastClient.newHazelcastClient(clientConfig);
        statusTopic = client.getTopic(Agent.AGENT_STABILIZER_TOPIC);
    }

    public static void main(String[] args) throws Exception {
        log.info("Hazelcast Stabilizer Console");
        log.info(format("Version: %s", getVersion()));
        log.info(format("STABILIZER_HOME: %s", STABILIZER_HOME));

        ConsoleOptionSpec optionSpec = new ConsoleOptionSpec();

        OptionSet options;
        Console console = new Console();

        try {
            options = optionSpec.parser.parse(args);

            if (options.has(optionSpec.helpSpec)) {
                optionSpec.parser.printHelpOn(System.out);
                System.exit(0);
            }

            console.setCleanWorkersHome(options.has(optionSpec.cleanWorkersHome));

            if (options.has(optionSpec.workerClassPathSpec)) {
                console.setWorkerClassPath(options.valueOf(optionSpec.workerClassPathSpec));
            }

            File consoleHzFile = new File(options.valueOf(optionSpec.consoleHzFileSpec));
            if (!consoleHzFile.exists()) {
                exitWithError(format("Console Hazelcast config file [%s] does not exist.\n", consoleHzFile));
            }
            console.consoleHzFile = consoleHzFile;
            console.verifyEnabled = options.valueOf(optionSpec.verifyEnabledSpec);
            console.monitorPerformance = options.valueOf(optionSpec.monitorPerformanceSpec);
            console.testStopTimeoutMs = options.valueOf(optionSpec.testStopTimeoutMsSpec);

            String workoutFileName = "workout.properties";
            List<String> workoutFiles = options.nonOptionArguments();
            if (workoutFiles.size() == 1) {
                workoutFileName = workoutFiles.get(0);
            } else if (workoutFiles.size() > 1) {
                exitWithError("Too many workout files specified.");
            }

            Workout workout = Workout.createWorkout(new File(workoutFileName));

            console.setWorkout(workout);
            workout.setDuration(getDuration(optionSpec, options));
            workout.setFailFast(options.valueOf(optionSpec.failFastSpec));

            WorkerVmSettings workerVmSettings = new WorkerVmSettings();
            workerVmSettings.setTrackLogging(options.has(optionSpec.workerTrackLoggingSpec));
            workerVmSettings.setVmOptions(options.valueOf(optionSpec.workerVmOptionsSpec));
            workerVmSettings.setWorkerCount(options.valueOf(optionSpec.workerCountSpec));
            workerVmSettings.setWorkerStartupTimeout(options.valueOf(optionSpec.workerStartupTimeoutSpec));
            workerVmSettings.setHzConfig(Utils.asText(buildWorkerHazelcastFile(optionSpec, options)));
            workerVmSettings.setRefreshJvm(options.valueOf(optionSpec.workerRefreshSpec));
            workerVmSettings.setJavaVendor(options.valueOf(optionSpec.workerJavaVendorSpec));
            workerVmSettings.setJavaVersion(options.valueOf(optionSpec.workerJavaVersionSpec));

            workout.setWorkerVmSettings(workerVmSettings);
        } catch (OptionException e) {
            Utils.exitWithError(e.getMessage() + ". Use --help to get overview of the help options.");
        }

        try {
            console.run();
            System.exit(0);
        } catch (Exception e) {
            log.severe("Failed to run workout", e);
            System.exit(1);
        }
    }

    private static int getDuration(ConsoleOptionSpec optionSpec, OptionSet options) {
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

    private static File buildWorkerHazelcastFile(ConsoleOptionSpec optionSpec, OptionSet options) {
        File workerHzFile = new File(options.valueOf(optionSpec.workerHzFileSpec));
        if (!workerHzFile.exists()) {
            exitWithError(format("Worker Hazelcast config file [%s] does not exist.\n", workerHzFile));
        }

        return workerHzFile;
    }
}
