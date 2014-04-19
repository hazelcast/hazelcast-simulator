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
import com.hazelcast.stabilizer.TestRecipe;
import com.hazelcast.stabilizer.Failure;
import com.hazelcast.stabilizer.FailureAlreadyThrownRuntimeException;
import com.hazelcast.stabilizer.JavaInstallationsRepository;
import com.hazelcast.stabilizer.Utils;
import com.hazelcast.stabilizer.tests.Workout;
import com.hazelcast.stabilizer.worker.WorkerJvm;
import com.hazelcast.stabilizer.worker.WorkerVmManager;
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
    public final static File workersHome = new File(getStablizerHome(), "workers");

    private File agentHzFile;
    private volatile HazelcastInstance agentHz;
    private volatile ITopic statusTopic;
    private volatile Workout workout;
    private volatile TestRecipe testRecipe;
    private final List<Failure> failures = Collections.synchronizedList(new LinkedList<Failure>());
    private IExecutorService agentExecutor;
    private WorkerVmManager workerVmManager;
    private final JavaInstallationsRepository repository = new JavaInstallationsRepository();
    private File javaInstallationsFile;

    public Workout getWorkout() {
        return workout;
    }

    public ITopic getStatusTopic() {
        return statusTopic;
    }

    public WorkerVmManager getWorkerVmManager() {
        return workerVmManager;
    }

    public HazelcastInstance getAgentHazelcastInstance() {
        return agentHz;
    }

    public TestRecipe getTestRecipe() {
        return testRecipe;
    }

    public void setTestRecipe(TestRecipe testRecipe) {
        this.testRecipe = testRecipe;
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
        getWorkerVmManager().destroyAll();
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
                if (messageObject instanceof Failure) {
                    Failure failure = (Failure) messageObject;
                    Member localMember = agentHz.getCluster().getLocalMember();
                    final boolean isLocal = localMember.getInetSocketAddress().equals(failure.getAgentAddress());
                    if (isLocal) {
                        log.severe("Local failure detected:" + failure);
                    } else {
                        log.severe("Remote failure detected:" + failure);
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

    public void publishFailure(Failure failure) {
        statusTopic.publish(failure);
    }

    public List shoutToWorkers(Callable task, String taskDescription) throws InterruptedException {
        Map<WorkerJvm, Future> futures = new HashMap<WorkerJvm, Future>();

         for (WorkerJvm workerJvm : workerVmManager.getWorkerJvms()) {
            Member member = workerJvm.getMember();
            if (member == null) continue;

            Future future = workerVmManager.getWorkerExecutor().submitToMember(task, member);
            futures.put(workerJvm, future);
        }

        List results = new LinkedList();
        for (Map.Entry<WorkerJvm, Future> entry : futures.entrySet()) {
            WorkerJvm workerJvm = entry.getKey();
            Future future = entry.getValue();
            try {
                Object result = future.get();
                results.add(result);
            } catch (ExecutionException e) {
                final Failure failure = new Failure(
                        taskDescription,
                        agentHz.getCluster().getLocalMember().getInetSocketAddress(),
                        workerJvm.getMember().getInetSocketAddress(),
                        workerJvm.getId(),
                        testRecipe,
                        e);
                publishFailure(failure);
                throw new FailureAlreadyThrownRuntimeException(e);
            }
        }
        return results;
    }


    public File getWorkoutHome() {
        Workout _workout = workout;
        if (_workout == null) {
            return null;
        }

        return new File(workersHome, _workout.getId());
    }

    public void cleanWorkers() throws IOException {
        for (File file : workersHome.listFiles()) {
            Utils.delete(file);
        }
    }

    public void initWorkout(Workout workout, byte[] content) throws IOException {
        failures.clear();

        this.workout = workout;
        this.testRecipe = null;

        File workoutDir = new File(workersHome, workout.getId());
        ensureExistingDirectory(workoutDir);

        File libDir = new File(workoutDir, "lib");
        ensureExistingDirectory(libDir);

        if (content != null) {
            Utils.unzip(content, libDir);
        }
    }

    public void start() throws Exception {
        ensureExistingDirectory(workersHome);

        workerVmManager = new WorkerVmManager(this);

        initAgentHazelcastInstance();

        repository.load(javaInstallationsFile);

        new Thread(new FailureMonitor(this)).start();

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
                "A property file containing the Java installations used by Workers launched by this Agent")
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
