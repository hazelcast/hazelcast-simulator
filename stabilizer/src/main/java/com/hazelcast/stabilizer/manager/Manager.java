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
package com.hazelcast.stabilizer.manager;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.XmlClientConfigBuilder;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.Member;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.ExerciseRecipe;
import com.hazelcast.stabilizer.HeartAttack;
import com.hazelcast.stabilizer.HeartAttackAlreadyThrownRuntimeException;
import com.hazelcast.stabilizer.Utils;
import com.hazelcast.stabilizer.coach.Coach;
import com.hazelcast.stabilizer.exercises.Workout;
import com.hazelcast.stabilizer.performance.NotAvailable;
import com.hazelcast.stabilizer.performance.Performance;
import com.hazelcast.stabilizer.tasks.CleanGym;
import com.hazelcast.stabilizer.tasks.GenericExerciseTask;
import com.hazelcast.stabilizer.tasks.InitExercise;
import com.hazelcast.stabilizer.tasks.InitWorkout;
import com.hazelcast.stabilizer.tasks.PrepareCoachForExercise;
import com.hazelcast.stabilizer.tasks.ShoutToTraineesTask;
import com.hazelcast.stabilizer.tasks.SpawnTrainees;
import com.hazelcast.stabilizer.tasks.StopTask;
import com.hazelcast.stabilizer.tasks.TellTrainee;
import com.hazelcast.stabilizer.tasks.TerminateWorkout;
import com.hazelcast.stabilizer.trainee.TraineeVmSettings;
import joptsimple.OptionException;
import joptsimple.OptionSet;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.hazelcast.stabilizer.Utils.exitWithError;
import static com.hazelcast.stabilizer.Utils.getStablizerHome;
import static com.hazelcast.stabilizer.Utils.getVersion;
import static com.hazelcast.stabilizer.Utils.secondsToHuman;
import static java.lang.String.format;
import static java.util.Collections.synchronizedList;

public class Manager {

    public final static File STABILIZER_HOME = getStablizerHome();
    private final static ILogger log = Logger.getLogger(Manager.class);

    private Workout workout;
    private File managerHzFile;
    private final List<HeartAttack> heartAttackList = synchronizedList(new LinkedList<HeartAttack>());
    private IExecutorService coachExecutor;
    private HazelcastInstance client;
    private ITopic statusTopic;
    private volatile ExerciseRecipe exerciseRecipe;
    private String traineeClassPath;
    private boolean cleanGym;
    private boolean monitorPerformance;
    private boolean verifyEnabled = true;
    private Integer exerciseStopTimeoutMs;

    public boolean isVerifyEnabled() {
        return verifyEnabled;
    }

    public void setVerifyEnabled(boolean verifyEnabled) {
        this.verifyEnabled = verifyEnabled;
    }

    public void setWorkout(Workout workout) {
        this.workout = workout;
    }

    public ExerciseRecipe getExerciseRecipe() {
        return exerciseRecipe;
    }

    public void setTraineeClassPath(String traineeClassPath) {
        this.traineeClassPath = traineeClassPath;
    }

    public String getTraineeClassPath() {
        return traineeClassPath;
    }

    public void setCleanGym(boolean cleanGym) {
        this.cleanGym = cleanGym;
    }

    public boolean isCleanGym() {
        return cleanGym;
    }

    private void run() throws Exception {
        initClient();

        if (cleanGym) {
            sendStatusUpdate("Starting cleanup gyms");
            submitToAllAndWait(coachExecutor, new CleanGym());
            sendStatusUpdate("Finished cleanup gyms");
        }

        byte[] bytes = createUpload();
        submitToAllAndWait(coachExecutor, new InitWorkout(workout, bytes));

        TraineeVmSettings traineeVmSettings = workout.getTraineeVmSettings();
        Set<Member> members = client.getCluster().getMembers();
        log.info(format("Trainee track logging: %s", traineeVmSettings.isTrackLogging()));
        log.info(format("Trainee's per coach: %s", traineeVmSettings.getTraineeCount()));
        log.info(format("Total number of coaches: %s", members.size()));
        log.info(format("Total number of trainees: %s", members.size() * traineeVmSettings.getTraineeCount()));

        ITopic heartAttackTopic = client.getTopic(Coach.COACH_STABILIZEr_TOPIC);
        heartAttackTopic.addMessageListener(new MessageListener() {
            @Override
            public void onMessage(Message message) {
                Object messageObject = message.getMessageObject();
                if (messageObject instanceof HeartAttack) {
                    HeartAttack heartAttack = (HeartAttack) messageObject;
                    heartAttackList.add(heartAttack);
                    log.severe("Remote machine heart attack detected:" + heartAttack);
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

        //the manager needs to sleep some to make sure that it will get heartattacks if they are there.
        log.info("Starting cooldown (10 sec)");
        Utils.sleepSeconds(10);
        log.info("Finished cooldown");

        client.getLifecycleService().shutdown();

        long elapsedMs = System.currentTimeMillis() - startMs;
        log.info(format("Total running time: %s seconds", elapsedMs / 1000));

        if (heartAttackList.isEmpty()) {
            log.info("-----------------------------------------------------------------------------");
            log.info("No heart attacks have been detected!");
            log.info("-----------------------------------------------------------------------------");
            System.exit(0);
        } else {
            StringBuilder sb = new StringBuilder();
            sb.append(heartAttackList.size()).append(" Heart attacks have been detected!!!\n");
            for (HeartAttack heartAttack : heartAttackList) {
                sb.append("-----------------------------------------------------------------------------\n");
                sb.append(heartAttack).append('\n');
            }
            sb.append("-----------------------------------------------------------------------------\n");
            log.severe(sb.toString());
            System.exit(1);
        }
    }

    private byte[] createUpload() throws IOException {
        if (traineeClassPath == null)
            return null;

        String[] parts = traineeClassPath.split(";");
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
        sendStatusUpdate(format("Exercises in workout: %s", workout.size()));
        sendStatusUpdate(format("Running time per exercise: %s ", secondsToHuman(workout.getDuration())));
        sendStatusUpdate(format("Expected total workout time: %s", secondsToHuman(workout.size() * workout.getDuration())));

        //we need to make sure that before we start, there are no trainees running anymore.
        //log.log(Level.INFO, "Ensuring trainee all killed");
        stopTrainees();
        startTrainees(workout.getTraineeVmSettings());

        for (ExerciseRecipe exerciseRecipe : workout.getExerciseRecipeList()) {
            boolean success = run(workout, exerciseRecipe);
            if (!success && workout.isFailFast()) {
                log.info("Aborting working due to failure");
                break;
            }

            if (!success || workout.getTraineeVmSettings().isRefreshJvm()) {
                stopTrainees();
                startTrainees(workout.getTraineeVmSettings());
            }
        }

        stopTrainees();
    }

    private boolean run(Workout workout, ExerciseRecipe exerciseRecipe) {
        sendStatusUpdate(format("Running exercise : %s", exerciseRecipe.getExerciseId()));

        this.exerciseRecipe = exerciseRecipe;
        int oldCount = heartAttackList.size();
        try {
            sendStatusUpdate(exerciseRecipe.toString());

            sendStatusUpdate("Starting Exercise initialization");
            submitToAllAndWait(coachExecutor, new PrepareCoachForExercise(exerciseRecipe));
            submitToAllTrainesAndWait(new InitExercise(exerciseRecipe), "exercise initializing");
            sendStatusUpdate("Completed Exercise initialization");

            sendStatusUpdate("Starting exercise local setup");
            submitToAllTrainesAndWait(new GenericExerciseTask("localSetup"), "exercise local setup");
            sendStatusUpdate("Completed exercise local setup");

            sendStatusUpdate("Starting exercise global setup");
            submitToOneTrainee(new GenericExerciseTask("globalSetup"));
            sendStatusUpdate("Completed exercise global setup");

            sendStatusUpdate("Starting exercise start");
            submitToAllTrainesAndWait(new GenericExerciseTask("start"), "exercise start");
            sendStatusUpdate("Completed exercise start");

            sendStatusUpdate(format("Exercise running for %s seconds", workout.getDuration()));
            sleepSeconds(workout.getDuration());
            sendStatusUpdate("Exercise finished running");

            sendStatusUpdate("Starting exercise stop");
            stopExercise();
            sendStatusUpdate("Completed exercise stop");

            if (monitorPerformance) {
                sendStatusUpdate(calcPerformance().toHumanString());
            }

            if (verifyEnabled) {
                sendStatusUpdate("Starting exercise global verify");
                submitToOneTrainee(new GenericExerciseTask("globalVerify"));
                sendStatusUpdate("Completed exercise global verify");

                sendStatusUpdate("Starting exercise local verify");
                submitToAllTrainesAndWait(new GenericExerciseTask("localVerify"), "exercise local verify");
                sendStatusUpdate("Completed exercise local verify");
            } else {
                sendStatusUpdate("Skipping exercise verification");
            }

            sendStatusUpdate("Starting exercise global teardown");
            submitToOneTrainee(new GenericExerciseTask("globalTearDown"));
            sendStatusUpdate("Finished exercise global teardown");

            sendStatusUpdate("Starting exercise local teardown");
            submitToAllTrainesAndWait(new GenericExerciseTask("localTearDown"), "exercise local tearDown");
            sendStatusUpdate("Completed exercise local teardown");

            return heartAttackList.size() == oldCount;
        } catch (Exception e) {
            log.severe("Failed", e);
            return false;
        }
    }

    private void stopExercise() throws ExecutionException, InterruptedException {
        Callable task = new ShoutToTraineesTask(new StopTask(exerciseStopTimeoutMs), "exercise stop");
        Map<Member, Future> map = coachExecutor.submitToAllMembers(task);
        getAllFutures(map.values());
    }

    public void sleepSeconds(int seconds) {
        int period = 30;
        int big = seconds / period;
        int small = seconds % period;

        for (int k = 1; k <= big; k++) {
            if (heartAttackList.size() > 0) {
                sendStatusUpdate("Heart attack detected, aborting execution of exercise");
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
        ShoutToTraineesTask task = new ShoutToTraineesTask(new GenericExerciseTask("calcPerformance"), "calcPerformance");
        Map<Member, Future<List<Performance>>> result = coachExecutor.submitToAllMembers(task);
        Performance performance = null;
        for (Future<List<Performance>> future : result.values()) {
            try {
                List<Performance> results = future.get();
                for (Performance p : results) {
                    if (performance == null) {
                        performance = p;
                    } else {
                        performance = performance.merge(p);
                    }
                }
            } catch (InterruptedException e) {
            } catch (ExecutionException e) {
                log.severe(e);
            }
        }
        return performance == null ? new NotAvailable() : performance;
    }

    private void stopTrainees() throws Exception {
        sendStatusUpdate("Stopping all remaining trainees");
        submitToAllAndWait(coachExecutor, new TerminateWorkout());
        sendStatusUpdate("All remaining trainees have been terminated");
    }

    private long startTrainees(TraineeVmSettings traineeVmSettings) throws Exception {
        long startMs = System.currentTimeMillis();
        final int traineeCount = traineeVmSettings.getTraineeCount();
        final int totalTraineeCount = traineeCount * client.getCluster().getMembers().size();
        log.info(format("Starting a grand total of %s Trainee Java Virtual Machines", totalTraineeCount));
        submitToAllAndWait(coachExecutor, new SpawnTrainees(traineeVmSettings));
        long durationMs = System.currentTimeMillis() - startMs;
        log.info((format("Finished starting a grand total of %s Trainees after %s ms\n", totalTraineeCount, durationMs)));
        return startMs;
    }

    private void sendStatusUpdate(String s) {
        try {
            statusTopic.publish(s);
        } catch (Exception e) {
            log.severe("Failed to echo to all members", e);
        }
    }

    private void submitToOneTrainee(Callable task) throws InterruptedException, ExecutionException {
        Future future = coachExecutor.submit(new TellTrainee(task));
        try {
            Object o = future.get(1000, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            if (!(e.getCause() instanceof HeartAttackAlreadyThrownRuntimeException)) {
                statusTopic.publish(new HeartAttack(null, null, null, null, getExerciseRecipe(), e));
            }
            throw new RuntimeException(e);
        } catch (TimeoutException e) {
            HeartAttack heartAttack = new HeartAttack("Timeout waiting for remote operation to complete",
                    null, null, null, getExerciseRecipe(), e);
            statusTopic.publish(heartAttack);
            throw new RuntimeException(e);
        }
    }

    private void submitToAllTrainesAndWait(Callable task, String taskDescription) throws InterruptedException, ExecutionException {
        submitToAllAndWait(coachExecutor, new ShoutToTraineesTask(task, taskDescription));
    }

    private void submitToAllAndWait(IExecutorService executorService, Callable task) throws InterruptedException, ExecutionException {
        Map<Member, Future> map = executorService.submitToAllMembers(task);
        getAllFutures(map.values());
    }

    private void getAllFutures(Collection<Future> futures) throws InterruptedException, ExecutionException {
        getAllFutures(futures, TimeUnit.SECONDS.toMillis(10000));
    }

    private void getAllFutures(Collection<Future> futures, long timeoutMs) throws InterruptedException, ExecutionException {
        for (Future future : futures) {
            try {
                //todo: we should calculate remaining timeoutMs
                Object o = future.get(timeoutMs, TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                HeartAttack heartAttack = new HeartAttack("Timeout waiting for remote operation to complete",
                        null, null, null, getExerciseRecipe(), e);
                statusTopic.publish(heartAttack);
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                if (!(e.getCause() instanceof HeartAttackAlreadyThrownRuntimeException)) {
                    statusTopic.publish(new HeartAttack(null, null, null, null, getExerciseRecipe(), e));
                }
                throw new RuntimeException(e);
            }
        }
    }

    private void initClient() throws FileNotFoundException {
        ClientConfig clientConfig = new XmlClientConfigBuilder(new FileInputStream(managerHzFile)).build();
        client = HazelcastClient.newHazelcastClient(clientConfig);
        coachExecutor = client.getExecutorService("Coach:Executor");
        statusTopic = client.getTopic(Coach.COACH_STABILIZEr_TOPIC);
    }

    public static void main(String[] args) throws Exception {
        log.info("Hazelcast Stabilizer Manager");
        log.info(format("Version: %s", getVersion()));
        log.info(format("STABILIZER_HOME: %s", STABILIZER_HOME));

        ManagerOptionSpec optionSpec = new ManagerOptionSpec();

        OptionSet options;
        Manager manager = new Manager();

        try {
            options = optionSpec.parser.parse(args);

            if (options.has(optionSpec.helpSpec)) {
                optionSpec.parser.printHelpOn(System.out);
                System.exit(0);
            }

            manager.setCleanGym(options.has(optionSpec.cleanGymSpec));

            if (options.has(optionSpec.traineeClassPathSpec)) {
                manager.setTraineeClassPath(options.valueOf(optionSpec.traineeClassPathSpec));
            }

            File managerHzFile = new File(options.valueOf(optionSpec.managerHzFileSpec));
            if (!managerHzFile.exists()) {
                exitWithError(format("Manager Hazelcast config file [%s] does not exist.\n", managerHzFile));
            }
            manager.managerHzFile = managerHzFile;
            manager.verifyEnabled = options.valueOf(optionSpec.verifyEnabledSpec);
            manager.monitorPerformance = options.valueOf(optionSpec.monitorPerformanceSpec);
            manager.exerciseStopTimeoutMs = options.valueOf(optionSpec.exerciseStopTimeoutMsSpec);

            String workoutFileName = "workout.properties";
            List<String> workoutFiles = options.nonOptionArguments();
            if (workoutFiles.size() == 1) {
                workoutFileName = workoutFiles.get(0);
            } else if (workoutFiles.size() > 1) {
                exitWithError("Too many workout files specified.");
            }

            Workout workout = Workout.createWorkout(new File(workoutFileName));

            manager.setWorkout(workout);
            workout.setDuration(getDuration(optionSpec, options));
            workout.setFailFast(options.valueOf(optionSpec.failFastSpec));

            TraineeVmSettings traineeVmSettings = new TraineeVmSettings();
            traineeVmSettings.setTrackLogging(options.has(optionSpec.traineeTrackLoggingSpec));
            traineeVmSettings.setVmOptions(options.valueOf(optionSpec.traineeVmOptionsSpec));
            traineeVmSettings.setTraineeCount(options.valueOf(optionSpec.traineeCountSpec));
            traineeVmSettings.setTraineeStartupTimeout(options.valueOf(optionSpec.traineeStartupTimeoutSpec));
            traineeVmSettings.setHzConfig(Utils.asText(buildTraineeHazelcastFile(optionSpec, options)));
            traineeVmSettings.setRefreshJvm(options.valueOf(optionSpec.traineeRefreshSpec));
            traineeVmSettings.setJavaVendor(options.valueOf(optionSpec.traineeJavaVendorSpec));
            traineeVmSettings.setJavaVersion(options.valueOf(optionSpec.traineeJavaVersionSpec));

            workout.setTraineeVmSettings(traineeVmSettings);
        } catch (OptionException e) {
            Utils.exitWithError(e.getMessage() + ". Use --help to get overview of the help options.");
        }

        try {
            manager.run();
            System.exit(0);
        } catch (Exception e) {
            log.severe("Failed to run workout", e);
            System.exit(1);
        }
    }

    private static int getDuration(ManagerOptionSpec optionSpec, OptionSet options) {
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
        }catch(NumberFormatException e){
            exitWithError(format("Failed to parse duration [%s], cause: %s", value,e.getMessage()));
            return -1;
        }
    }

    private static File buildTraineeHazelcastFile(ManagerOptionSpec optionSpec, OptionSet options) {
        File traineeHzFile = new File(options.valueOf(optionSpec.traineeHzFileSpec));
        if (!traineeHzFile.exists()) {
            exitWithError(format("Trainee Hazelcast config file [%s] does not exist.\n", traineeHzFile));
        }

        return traineeHzFile;
    }
}
