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
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.JavaInstallationsRepository;
import com.hazelcast.stabilizer.TestRecipe;
import com.hazelcast.stabilizer.Utils;
import com.hazelcast.stabilizer.tests.Workout;
import joptsimple.OptionException;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;

import static com.hazelcast.stabilizer.Utils.closeQuietly;
import static com.hazelcast.stabilizer.Utils.ensureExistingDirectory;
import static com.hazelcast.stabilizer.Utils.exitWithError;
import static com.hazelcast.stabilizer.Utils.getHostAddress;
import static com.hazelcast.stabilizer.Utils.getStablizerHome;
import static com.hazelcast.stabilizer.Utils.getVersion;
import static com.hazelcast.stabilizer.agent.AgentCli.init;
import static java.lang.String.format;

public class Agent {

    public static final String BASE_URI = format("http://%s:8080/", getHostAddress());

    private final static ILogger log = Logger.getLogger(Agent.class);
    public final static File STABILIZER_HOME = getStablizerHome();
    public static final String KEY_AGENT = "Agent";

    //for the agentservice
    public static volatile Agent agent;

    //cli properties
    public File agentHzFile;
    public File javaInstallationsFile;

    //internal state
    private volatile HazelcastInstance agentHz;
    private volatile Workout workout;
    private volatile TestRecipe testRecipe;
    private final WorkerJvmManager workerJvmManager = new WorkerJvmManager(this);
    private final JavaInstallationsRepository repository = new JavaInstallationsRepository();
    private final FailureMonitor failureMonitor = new FailureMonitor(this);

    public Agent() {
        agent = this;
    }

    public void echo(String msg) {
        log.info(msg);
    }

    public Workout getWorkout() {
        return workout;
    }

    public File getWorkoutHome() {
        Workout _workout = agent.getWorkout();
        if (_workout == null) {
            return null;
        }

        return new File(WorkerJvmManager.WORKERS_HOME, _workout.id);
    }

    public FailureMonitor getFailureMonitor() {
        return failureMonitor;
    }

    public WorkerJvmManager getWorkerJvmManager() {
        return workerJvmManager;
    }

    public TestRecipe getTestRecipe() {
        return testRecipe;
    }

    public void setTestRecipe(TestRecipe testRecipe) {
        this.testRecipe = testRecipe;
    }

    public HazelcastInstance getAgentHz() {
        return agentHz;
    }

    public JavaInstallationsRepository getJavaInstallationRepository() {
        return repository;
    }

    //todo: this will go.
    protected HazelcastInstance startAgentHazelcastInstance() {
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

        return agentHz;
    }

    public void initWorkout(Workout workout, byte[] content) throws IOException {
        this.workout = workout;
        this.testRecipe = null;

        File workoutDir = new File(WorkerJvmManager.WORKERS_HOME, workout.id);
        ensureExistingDirectory(workoutDir);

        File libDir = new File(workoutDir, "lib");
        ensureExistingDirectory(libDir);

        if (content != null) {
            Utils.unzip(content, libDir);
        }
    }

    public void start() throws Exception {
        ensureExistingDirectory(WorkerJvmManager.WORKERS_HOME);

        startRestServer();

        startAgentHazelcastInstance();

        repository.load(javaInstallationsFile);

        failureMonitor.start();

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

        try {
            Agent agent = new Agent();
            init(agent, args);
            agent.start();
        } catch (OptionException e) {
            exitWithError(e.getMessage() + "\nUse --help to get overview of the help options.");
        }
    }
}
