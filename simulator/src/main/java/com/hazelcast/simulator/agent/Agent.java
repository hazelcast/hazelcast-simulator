/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.simulator.agent;

import com.hazelcast.simulator.agent.remoting.AgentMessageProcessor;
import com.hazelcast.simulator.agent.remoting.AgentRemoteService;
import com.hazelcast.simulator.agent.workerjvm.WorkerJvmFailureMonitor;
import com.hazelcast.simulator.agent.workerjvm.WorkerJvmManager;
import com.hazelcast.simulator.coordinator.Coordinator;
import com.hazelcast.simulator.test.TestSuite;
import com.hazelcast.simulator.utils.CommandLineExitException;
import org.apache.log4j.Logger;

import java.io.File;

import static com.hazelcast.simulator.common.GitInfo.getBuildTime;
import static com.hazelcast.simulator.common.GitInfo.getCommitIdAbbrev;
import static com.hazelcast.simulator.utils.CommonUtils.exitWithError;
import static com.hazelcast.simulator.utils.CommonUtils.getSimulatorVersion;
import static com.hazelcast.simulator.utils.FileUtils.ensureExistingDirectory;
import static com.hazelcast.simulator.utils.FileUtils.getSimulatorHome;
import static java.lang.String.format;

public class Agent {

    private static final Logger LOGGER = Logger.getLogger(Coordinator.class);

    String bindAddress;
    String cloudIdentity;
    String cloudCredential;
    String cloudProvider;

    // internal state
    volatile long lastUsed = System.currentTimeMillis();

    private final WorkerJvmManager workerJvmManager = new WorkerJvmManager(this);
    private final WorkerJvmFailureMonitor workerJvmFailureMonitor = new WorkerJvmFailureMonitor(this);
    private final HarakiriMonitor harakiriMonitor = new HarakiriMonitor(this);

    private volatile TestSuite testSuite;

    public void echo(String msg) {
        LOGGER.info(msg);
    }

    public String getBindAddress() {
        return bindAddress;
    }

    public TestSuite getTestSuite() {
        return testSuite;
    }

    public void signalUsed() {
        lastUsed = System.currentTimeMillis();
    }

    public File getTestSuiteDir() {
        TestSuite testSuite = this.testSuite;
        if (testSuite == null) {
            return null;
        }
        return new File(WorkerJvmManager.WORKERS_HOME, testSuite.id);
    }

    public WorkerJvmFailureMonitor getWorkerJvmFailureMonitor() {
        return workerJvmFailureMonitor;
    }

    public WorkerJvmManager getWorkerJvmManager() {
        return workerJvmManager;
    }

    public void initTestSuite(TestSuite testSuite) {
        this.testSuite = testSuite;

        File testSuiteDir = new File(WorkerJvmManager.WORKERS_HOME, testSuite.id);
        ensureExistingDirectory(testSuiteDir);

        File libDir = new File(testSuiteDir, "lib");
        ensureExistingDirectory(libDir);
    }

    private void start() {
        ensureExistingDirectory(WorkerJvmManager.WORKERS_HOME);

        startRestServer();
        workerJvmFailureMonitor.start();
        workerJvmManager.start();
        harakiriMonitor.start();

        LOGGER.info("Simulator Agent is ready for action");
    }

    private void startRestServer() {
        try {
            AgentMessageProcessor agentMessageProcessor = new AgentMessageProcessor(workerJvmManager);
            AgentRemoteService agentRemoteService = new AgentRemoteService(this, agentMessageProcessor);
            agentRemoteService.start();
        } catch (Exception e) {
            throw new CommandLineExitException("Failed to start REST server", e);
        }
    }

    public static void main(String[] args) {
        createAgent(args);
    }

    static void createAgent(String[] args) {
        try {
            LOGGER.info("Simulator Agent");
            LOGGER.info(format("Version: %s, Commit: %s, Build Time: %s",
                    getSimulatorVersion(),
                    getCommitIdAbbrev(),
                    getBuildTime()));
            LOGGER.info(format("SIMULATOR_HOME: %s%n", getSimulatorHome()));
            logInterestingSystemProperties();

            Agent agent = new Agent();
            AgentCli.init(agent, args);

            LOGGER.info("Bind address: " + agent.bindAddress);
            LOGGER.info("CloudIdentity: " + agent.cloudIdentity);
            LOGGER.info("CloudCredential: " + agent.cloudCredential);
            LOGGER.info("CloudProvider: " + agent.cloudProvider);

            agent.start();
        } catch (Exception e) {
            if (!(e instanceof CommandLineExitException)) {
                LOGGER.fatal("Use --help to get overview of the help options", e);
            }
            exitWithError(LOGGER, e.getMessage());
        }
    }

    private static void logInterestingSystemProperties() {
        logSystemProperty("java.class.path");
        logSystemProperty("java.home");
        logSystemProperty("java.vendor");
        logSystemProperty("java.vendor.url");
        logSystemProperty("sun.java.command");
        logSystemProperty("java.version");
        logSystemProperty("os.arch");
        logSystemProperty("os.name");
        logSystemProperty("os.version");
        logSystemProperty("user.dir");
        logSystemProperty("user.home");
        logSystemProperty("user.name");
        logSystemProperty("SIMULATOR_HOME");
    }

    private static void logSystemProperty(String name) {
        LOGGER.info(format("%s=%s", name, System.getProperty(name)));
    }
}
