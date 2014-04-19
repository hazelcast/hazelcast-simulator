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
package com.hazelcast.stabilizer.agent;

import com.hazelcast.config.Config;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.Hazelcast;
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
import com.hazelcast.stabilizer.JavaInstallationsRepository;
import com.hazelcast.stabilizer.Utils;
import com.hazelcast.stabilizer.exercises.Workout;
import com.hazelcast.stabilizer.trainee.TraineeVm;
import com.hazelcast.stabilizer.trainee.TraineeVmManager;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.hazelcast.stabilizer.Utils.closeQuietly;
import static com.hazelcast.stabilizer.Utils.ensureExistingDirectory;
import static com.hazelcast.stabilizer.Utils.exitWithError;
import static com.hazelcast.stabilizer.Utils.getStablizerHome;
import static com.hazelcast.stabilizer.Utils.getVersion;
import static java.lang.String.format;

public class Agent {

    private final static ILogger log = Logger.getLogger(Agent.class);
    private final static File STABILIZER_HOME = getStablizerHome();

    public static final String KEY_AGENT = "Agent";
    public static final String AGENT_STABILIZER_TOPIC = "Agent:stabilizerTopic";

    public final static File stabilizerHome = getStablizerHome();
    public final static File gymHome = new File(getStablizerHome(), "gym");

    private File agentHzFile;
    private volatile HazelcastInstance agentHz;
    private volatile ITopic statusTopic;
    private volatile Workout workout;
    private volatile ExerciseRecipe exerciseRecipe;
    private final List<HeartAttack> heartAttacks = Collections.synchronizedList(new LinkedList<HeartAttack>());
    private IExecutorService agentExecutor;
    private TraineeVmManager traineeVmManager;
    private final JavaInstallationsRepository repository = new JavaInstallationsRepository();
    private File javaInstallationsFile;

    public Workout getWorkout() {
        return workout;
    }

    public ITopic getStatusTopic() {
        return statusTopic;
    }

    public TraineeVmManager getTraineeVmManager() {
        return traineeVmManager;
    }

    public HazelcastInstance getAgentHazelcastInstance() {
        return agentHz;
    }

    public ExerciseRecipe getExerciseRecipe() {
        return exerciseRecipe;
    }

    public void setExerciseRecipe(ExerciseRecipe exerciseRecipe) {
        this.exerciseRecipe = exerciseRecipe;
    }

    public void setAgentHzFile(File agentHzFile) {
        this.agentHzFile = agentHzFile;
    }

    public HazelcastInstance getAgentHz() {
        return agentHz;
    }

    public File getAgentHzFile() {
        return agentHzFile;
    }

    public void setJavaInstallationsFile(File javaInstallationsFile) {
        this.javaInstallationsFile = javaInstallationsFile;
    }

    public File getJavaInstallationsFile() {
        return javaInstallationsFile;
    }

    public JavaInstallationsRepository getJavaInstallationRepository() {
        return repository;
    }

    public void terminateWorkout() {
        log.info("Terminating workout");
        getTraineeVmManager().destroyAll();
        log.info("Finished terminating workout");
    }

    protected HazelcastInstance initAgentHazelcastInstance() {
        FileInputStream in;
        try {
            in = new FileInputStream(agentHzFile);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }

        Config config;
        try {
            config = new XmlConfigBuilder(in).build();
        } finally {
            closeQuietly(in);
        }
        config.getUserContext().put(KEY_AGENT, this);
        agentHz = Hazelcast.newHazelcastInstance(config);
        statusTopic = agentHz.getTopic(AGENT_STABILIZER_TOPIC);
        statusTopic.addMessageListener(new MessageListener() {
            @Override
            public void onMessage(Message message) {
                Object messageObject = message.getMessageObject();
                if (messageObject instanceof HeartAttack) {
                    HeartAttack heartAttack = (HeartAttack) messageObject;
                    Member localMember = agentHz.getCluster().getLocalMember();
                    final boolean isLocal = localMember.getInetSocketAddress().equals(heartAttack.getAgentAddress());
                    if (isLocal) {
                        log.severe("Local heart attack detected:" + heartAttack);
                    } else {
                        log.severe("Remote machine heart attack detected:" + heartAttack);
                    }
                } else if (messageObject instanceof Exception) {
                    Exception e = (Exception) messageObject;
                    log.severe(e);
                } else {
                    log.info(messageObject.toString());
                }
            }
        });
        agentExecutor = agentHz.getExecutorService("Agent:Executor");

        return agentHz;
    }

    public void heartAttack(HeartAttack heartAttack) {
        statusTopic.publish(heartAttack);
    }

    public List shoutToTrainees(Callable task, String taskDescription) throws InterruptedException {
        Map<TraineeVm, Future> futures = new HashMap<TraineeVm, Future>();

         for (TraineeVm traineeJvm : traineeVmManager.getTraineeJvms()) {
            Member member = traineeJvm.getMember();
            if (member == null) continue;

            Future future = traineeVmManager.getTraineeExecutor().submitToMember(task, member);
            futures.put(traineeJvm, future);
        }

        List results = new LinkedList();
        for (Map.Entry<TraineeVm, Future> entry : futures.entrySet()) {
            TraineeVm traineeJvm = entry.getKey();
            Future future = entry.getValue();
            try {
                Object result = future.get();
                results.add(result);
            } catch (ExecutionException e) {
                final HeartAttack heartAttack = new HeartAttack(
                        taskDescription,
                        agentHz.getCluster().getLocalMember().getInetSocketAddress(),
                        traineeJvm.getMember().getInetSocketAddress(),
                        traineeJvm.getId(),
                        exerciseRecipe,
                        e);
                heartAttack(heartAttack);
                throw new HeartAttackAlreadyThrownRuntimeException(e);
            }
        }
        return results;
    }


    public File getWorkoutHome() {
        Workout _workout = workout;
        if (_workout == null) {
            return null;
        }

        return new File(gymHome, _workout.getId());
    }

    public void cleanGym() throws IOException {
        for (File file : gymHome.listFiles()) {
            Utils.delete(file);
        }
    }

    public void initWorkout(Workout workout, byte[] content) throws IOException {
        heartAttacks.clear();

        this.workout = workout;
        this.exerciseRecipe = null;

        File workoutDir = new File(gymHome, workout.getId());
        ensureExistingDirectory(workoutDir);

        File libDir = new File(workoutDir, "lib");
        ensureExistingDirectory(libDir);

        if (content != null) {
            Utils.unzip(content, libDir);
        }
    }

    public void start() throws Exception {
        ensureExistingDirectory(gymHome);

        traineeVmManager = new TraineeVmManager(this);

        initAgentHazelcastInstance();

        repository.load(javaInstallationsFile);

        new Thread(new HeartAttackMonitor(this)).start();

        log.info("Hazelcast Assistant Agent is Ready for action");
    }

    public static void main(String[] args) throws Exception {
        log.info("Hazelcast Stabilizer Agent");
        log.info(format("Version: %s\n", getVersion()));
        log.info(format("STABILIZER_HOME: %s\n", stabilizerHome));

        OptionParser parser = new OptionParser();
        OptionSpec helpSpec = parser.accepts("help", "Show help").forHelp();
        OptionSpec<String> agentHzFileSpec = parser.accepts("agentHzFile",
                "The Hazelcast xml configuration file for the agent")
                .withRequiredArg().ofType(String.class)
                .defaultsTo(stabilizerHome + File.separator + "conf" + File.separator + "agent-hazelcast.xml");
        OptionSpec<String> javaInstallationsFileSpec = parser.accepts("javaInstallationsFile",
                "A property file containing the Java installations used by Trainees launched by this Agent")
                .withRequiredArg().ofType(String.class)
                .defaultsTo(STABILIZER_HOME + File.separator + "conf" + File.separator + "java-installations.properties");

        try {
            OptionSet options = parser.parse(args);

            if (options.has(helpSpec)) {
                parser.printHelpOn(System.out);
                System.exit(0);
            }
            Agent agent = new Agent();

            File javaInstallationsFile = new File(options.valueOf(javaInstallationsFileSpec));
            if (!javaInstallationsFile.exists()) {
                exitWithError(format("Java Installations config file [%s] does not exist\n", javaInstallationsFile));
            }
            agent.setJavaInstallationsFile(javaInstallationsFile);

            File agentHzFile = new File(options.valueOf(agentHzFileSpec));
            if (!agentHzFile.exists()) {
                exitWithError(format("Agent Hazelcast config file [%s] does not exist\n", agentHzFile));
            }
            agent.setAgentHzFile(agentHzFile);
            agent.start();
        } catch (OptionException e) {
            exitWithError(e.getMessage() + "\nUse --help to get overview of the help options.");
        }
    }

}
