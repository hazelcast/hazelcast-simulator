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
import com.hazelcast.core.ITopic;
import com.hazelcast.core.Member;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.Failure;
import com.hazelcast.stabilizer.FailureAlreadyThrownRuntimeException;
import com.hazelcast.stabilizer.JavaInstallationsRepository;
import com.hazelcast.stabilizer.TestRecipe;
import com.hazelcast.stabilizer.Utils;
import com.hazelcast.stabilizer.tests.Workout;
import com.hazelcast.stabilizer.worker.WorkerJvm;
import com.hazelcast.stabilizer.worker.WorkerVmManager;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
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
import static com.hazelcast.stabilizer.Utils.getHostAddress;
import static com.hazelcast.stabilizer.Utils.getStablizerHome;
import static com.hazelcast.stabilizer.Utils.getVersion;
import static java.lang.String.format;

public class Agent {

    public static final String BASE_URI = format("http://%s:8080/agent/", getHostAddress());

    private final static ILogger log = Logger.getLogger(Agent.class);
    private final static File STABILIZER_HOME = getStablizerHome();
    public static final String KEY_AGENT = "Agent";
    public static final String AGENT_STABILIZER_TOPIC = "Agent:stabilizerTopic";
    public final static File WORKERS_HOME = new File(getStablizerHome(), "workers");

    public static volatile Agent agent;

    private File agentHzFile;
    private volatile HazelcastInstance agentHz;
    private volatile ITopic statusTopic;
    private volatile Workout workout;
    private volatile TestRecipe testRecipe;
    private final List<Failure> failures = Collections.synchronizedList(new LinkedList<Failure>());
    private final WorkerVmManager workerVmManager = new WorkerVmManager(this);
    private final JavaInstallationsRepository repository = new JavaInstallationsRepository();
    private File javaInstallationsFile;

    public Agent() {
        agent = this;
    }

    public void echo(String msg) {
        log.info(msg);
    }

    public Workout getWorkout() {
        return workout;
    }

    public WorkerVmManager getWorkerVmManager() {
        return workerVmManager;
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

    public void setJavaInstallationsFile(File javaInstallationsFile) {
        this.javaInstallationsFile = javaInstallationsFile;
    }

    public JavaInstallationsRepository getJavaInstallationRepository() {
        return repository;
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

        return new File(WORKERS_HOME, _workout.getId());
    }

    public void cleanWorkersHome() throws IOException {
        for (File file : WORKERS_HOME.listFiles()) {
            Utils.delete(file);
        }
    }

    public void initWorkout(Workout workout, byte[] content) throws IOException {
        failures.clear();

        this.workout = workout;
        this.testRecipe = null;

        File workoutDir = new File(WORKERS_HOME, workout.getId());
        ensureExistingDirectory(workoutDir);

        File libDir = new File(workoutDir, "lib");
        ensureExistingDirectory(libDir);

        if (content != null) {
            Utils.unzip(content, libDir);
        }
    }

    public void start() throws Exception {
        ensureExistingDirectory(WORKERS_HOME);

        startRestServer();

        initAgentHazelcastInstance();

        repository.load(javaInstallationsFile);

        new Thread(new FailureMonitor(this)).start();

        log.info("Stabilizer Agent is ready for action");
    }

    private void startRestServer() throws IOException {
        ResourceConfig rc = new ResourceConfig().packages("com.hazelcast.stabilizer.agent");
        HttpServer server = GrizzlyHttpServerFactory.createHttpServer(URI.create(BASE_URI), rc);
        server.start();
    }

    public static void main(String[] args) throws Exception {
        log.info("Stabilizer Agent");
        log.info(format("Version: %s\n", getVersion()));
        log.info(format("STABILIZER_HOME: %s\n", STABILIZER_HOME));

        OptionParser parser = new OptionParser();
        OptionSpec helpSpec = parser.accepts("help", "Show help").forHelp();
        OptionSpec<String> agentHzFileSpec = parser.accepts("agentHzFile",
                "The Hazelcast xml configuration file for the agent")
                .withRequiredArg().ofType(String.class)
                .defaultsTo(STABILIZER_HOME + File.separator + "conf" + File.separator + "agent-hazelcast.xml");
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

            agent.setJavaInstallationsFile(Utils.getFile(javaInstallationsFileSpec, options, "Java Installations config file"));
            agent.setAgentHzFile(Utils.getFile(agentHzFileSpec, options, "Agent Hazelcast config file"));
            agent.start();
        } catch (OptionException e) {
            exitWithError(e.getMessage() + "\nUse --help to get overview of the help options.");
        }
    }
}
