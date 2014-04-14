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
package com.hazelcast.stabilizer;

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
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.hazelcast.stabilizer.Utils.exitWithError;
import static com.hazelcast.stabilizer.Utils.getStablizerHome;
import static com.hazelcast.stabilizer.Utils.getVersion;
import static com.hazelcast.stabilizer.Utils.loadProperties;
import static com.hazelcast.stabilizer.Utils.secondsToHuman;
import static java.lang.String.format;

public class Manager {

    private final static File STABILIZER_HOME = getStablizerHome();
    private final static ILogger log = Logger.getLogger(Manager.class);

    private Workout workout;
    private File managerHzFile;
    private final List<HeartAttack> heartAttackList = Collections.synchronizedList(new LinkedList<HeartAttack>());
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
            statusTopic.publish(new HeartAttack("Timeout waiting for remote operation to complete", null, null, null, getExerciseRecipe(), e));
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
                statusTopic.publish(new HeartAttack("Timeout waiting for remote operation to complete", null, null, null, getExerciseRecipe(), e));
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

        OptionParser parser = new OptionParser();
        OptionSpec cleanGymSpec = parser.accepts("cleanGym", "Cleans the gym directory on all coaches");

        OptionSpec<Integer> durationSpec = parser.accepts("duration", "Number of seconds to run per workout)")
                .withRequiredArg().ofType(Integer.class).defaultsTo(60);
        OptionSpec traineeTrackLoggingSpec = parser.accepts("traineeTrackLogging", "If the coach is tracking trainee logging");
        OptionSpec<Integer> traineeCountSpec = parser.accepts("traineeVmCount", "Number of trainee VM's per coach")
                .withRequiredArg().ofType(Integer.class).defaultsTo(1);
        OptionSpec<String> traineeClassPathSpec = parser.accepts("traineeClassPath", "A file/directory containing the " +
                "classes/jars/resources that are going to be uploaded to the coaches. " +
                "Use ';' as separator for multiple entries. Wildcard '*' can also be used.")
                .withRequiredArg().ofType(String.class);
        OptionSpec<Integer> traineeStartupTimeoutSpec = parser.accepts("traineeStartupTimeout", "The startup timeout in " +
                "seconds for a trainee")
                .withRequiredArg().ofType(Integer.class).defaultsTo(60);
        OptionSpec<Boolean> monitorPerformanceSpec = parser.accepts("monitorPerformance", "If performance monitoring " +
                "should be done")
                .withRequiredArg().ofType(Boolean.class).defaultsTo(false);
        OptionSpec<Boolean> verifyEnabledSpec = parser.accepts("verifyEnabled", "If exercise should be verified")
                .withRequiredArg().ofType(Boolean.class).defaultsTo(true);
        OptionSpec<Boolean> traineeRefreshSpec = parser.accepts("traineeFresh", "If the trainee VM's should be replaced " +
                "after every workout")
                .withRequiredArg().ofType(Boolean.class).defaultsTo(false);
        OptionSpec<Boolean> failFastSpec = parser.accepts("failFast", "It the workout should fail immediately when an " +
                "exercise from a workout fails instead of continuing ")
                .withRequiredArg().ofType(Boolean.class).defaultsTo(true);
        OptionSpec<String> traineeVmOptionsSpec = parser.accepts("traineeVmOptions", "Trainee VM options (quotes " +
                "can be used)")
                .withRequiredArg().ofType(String.class).defaultsTo("");
        OptionSpec<String> traineeHzFileSpec = parser.accepts("traineeHzFile", "The Hazelcast xml configuration file " +
                "for the trainee")
                .withRequiredArg().ofType(String.class).defaultsTo(
                        STABILIZER_HOME + File.separator + "conf" + File.separator + "trainee-hazelcast.xml");
        OptionSpec<String> managerHzFileSpec = parser.accepts(
                "managerHzFile", "The client Hazelcast xml configuration file for the manager")
                .withRequiredArg().ofType(String.class).defaultsTo(
                        STABILIZER_HOME + File.separator + "conf" + File.separator + "manager-hazelcast.xml");
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

        OptionSet options;
        Manager manager = new Manager();

        try {
            options = parser.parse(args);

            if (options.has(helpSpec)) {
                parser.printHelpOn(System.out);
                System.exit(0);
            }

            manager.setCleanGym(options.has(cleanGymSpec));

            if (options.has(traineeClassPathSpec)) {
                manager.setTraineeClassPath(options.valueOf(traineeClassPathSpec));
            }

            File managerHzFile = new File(options.valueOf(managerHzFileSpec));
            if (!managerHzFile.exists()) {
                exitWithError(format("Manager Hazelcast config file [%s] does not exist.\n", managerHzFile));
            }
            manager.managerHzFile = managerHzFile;
            manager.verifyEnabled = options.valueOf(verifyEnabledSpec);
            manager.monitorPerformance = options.valueOf(monitorPerformanceSpec);
            manager.exerciseStopTimeoutMs = options.valueOf(exerciseStopTimeoutMsSpec);

            String workoutFileName = "workout.properties";
            List<String> workoutFiles = options.nonOptionArguments();
            if (workoutFiles.size() == 1) {
                workoutFileName = workoutFiles.get(0);
            } else if (workoutFiles.size() > 1) {
                exitWithError("Too many workout files specified.");
            }

            Workout workout = createWorkout(new File(workoutFileName));

            manager.setWorkout(workout);
            workout.setDuration(options.valueOf(durationSpec));
            workout.setFailFast(options.valueOf(failFastSpec));

            File traineeHzFile = new File(options.valueOf(traineeHzFileSpec));
            if (!traineeHzFile.exists()) {
                exitWithError(format("Trainee Hazelcast config file [%s] does not exist.\n", traineeHzFile));
            }

            TraineeVmSettings traineeVmSettings = new TraineeVmSettings();
            traineeVmSettings.setTrackLogging(options.has(traineeTrackLoggingSpec));
            traineeVmSettings.setVmOptions(options.valueOf(traineeVmOptionsSpec));
            traineeVmSettings.setTraineeCount(options.valueOf(traineeCountSpec));
            traineeVmSettings.setTraineeStartupTimeout(options.valueOf(traineeStartupTimeoutSpec));
            traineeVmSettings.setHzConfig(Utils.asText(traineeHzFile));
            traineeVmSettings.setRefreshJvm(options.valueOf(traineeRefreshSpec));
            traineeVmSettings.setJavaVendor(options.valueOf(traineeJavaVendorSpec));
            traineeVmSettings.setJavaVersion(options.valueOf(traineeJavaVersionSpec));

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


    private static Workout createWorkout(File file) throws Exception {
        Properties properties = loadProperties(file);

        Map<String, ExerciseRecipe> recipies = new HashMap<String, ExerciseRecipe>();
        for (String property : properties.stringPropertyNames()) {
            String value = (String) properties.get(property);
            int indexOfDot = property.indexOf(".");

            String recipeId = "";
            String field = property;
            if (indexOfDot > -1) {
                recipeId = property.substring(0, indexOfDot);
                field = property.substring(indexOfDot + 1);
            }

            ExerciseRecipe recipe = recipies.get(recipeId);
            if (recipe == null) {
                recipe = new ExerciseRecipe();
                recipies.put(recipeId, recipe);
            }

            recipe.setProperty(field, value);
        }

        List<String> recipeIds = new LinkedList<String>(recipies.keySet());
        Collections.sort(recipeIds);

        Workout workout = new Workout();
        for (String recipeId : recipeIds) {
            ExerciseRecipe recipe = recipies.get(recipeId);
            if (recipe.getClassname() == null) {
                if ("".equals(recipeId)) {
                    throw new RuntimeException(format("There is no class set for the in property file [%s]." +
                                    "Add class=YourExerciseClass",
                            file.getAbsolutePath()
                    ));
                } else {
                    throw new RuntimeException(format("There is no class set for exercise [%s] in property file [%s]." +
                                    "Add %s.class=YourExerciseClass",
                            recipeId, file.getAbsolutePath(), recipeId
                    ));
                }
            }
            workout.addExercise(recipe);
        }
        return workout;
    }
}
