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

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.TestRecipe;
import com.hazelcast.stabilizer.Utils;
import com.hazelcast.stabilizer.tests.TestSuite;
import joptsimple.OptionException;

import java.io.File;
import java.io.IOException;

import static com.hazelcast.stabilizer.Utils.ensureExistingDirectory;
import static com.hazelcast.stabilizer.Utils.exitWithError;
import static com.hazelcast.stabilizer.Utils.getStablizerHome;
import static com.hazelcast.stabilizer.Utils.getVersion;
import static com.hazelcast.stabilizer.agent.AgentCli.init;
import static java.lang.String.format;

public class Agent {

    private final static ILogger log = Logger.getLogger(Agent.class);
    public final static File STABILIZER_HOME = getStablizerHome();

    //cli properties
    public File javaInstallationsFile;

    //internal state
    private volatile TestSuite testSuite;
    private volatile TestRecipe testRecipe;
    private final WorkerJvmManager workerJvmManager = new WorkerJvmManager(this);
    private final JavaInstallationsRepository repository = new JavaInstallationsRepository();
    private final WorkerJvmFailureMonitor workerJvmFailureMonitor = new WorkerJvmFailureMonitor(this);

    public void echo(String msg) {
        log.info(msg);
    }

    public TestSuite getTestSuite() {
        return testSuite;
    }

    public File getTestSuiteDir() {
        TestSuite _testSuite = testSuite;
        if (_testSuite == null) {
            return null;
        }

        return new File(WorkerJvmManager.WORKERS_HOME, _testSuite.id);
    }

    public WorkerJvmFailureMonitor getWorkerJvmFailureMonitor() {
        return workerJvmFailureMonitor;
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

    public JavaInstallationsRepository getJavaInstallationRepository() {
        return repository;
    }

    public void initTestSuite(TestSuite testSuite, byte[] content) throws IOException {
        this.testSuite = testSuite;
        this.testRecipe = null;

        File testSuiteDir = new File(WorkerJvmManager.WORKERS_HOME, testSuite.id);
        ensureExistingDirectory(testSuiteDir);

        File libDir = new File(testSuiteDir, "lib");
        ensureExistingDirectory(libDir);

        if (content != null) {
            Utils.unzip(content, libDir);
        }
    }

    public void start() throws Exception {
        ensureExistingDirectory(WorkerJvmManager.WORKERS_HOME);

        startRestServer();

        repository.load(javaInstallationsFile);

        workerJvmFailureMonitor.start();

        workerJvmManager.start();

        log.info("Stabilizer Agent is ready for action");
    }

    private void startRestServer() throws IOException {
        AgentRemoteService agentRemoteService = new AgentRemoteService(this);
        agentRemoteService.start();
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
