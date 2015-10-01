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
import com.hazelcast.simulator.agent.workerjvm.WorkerJvm;
import com.hazelcast.simulator.agent.workerjvm.WorkerJvmFailureMonitor;
import com.hazelcast.simulator.agent.workerjvm.WorkerJvmManager;
import com.hazelcast.simulator.common.CoordinatorLogger;
import com.hazelcast.simulator.coordinator.Coordinator;
import com.hazelcast.simulator.protocol.configuration.Ports;
import com.hazelcast.simulator.protocol.connector.AgentConnector;
import com.hazelcast.simulator.test.TestSuite;
import com.hazelcast.simulator.utils.CommandLineExitException;
import com.hazelcast.util.EmptyStatement;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.simulator.common.GitInfo.getBuildTime;
import static com.hazelcast.simulator.common.GitInfo.getCommitIdAbbrev;
import static com.hazelcast.simulator.utils.CommonUtils.exitWithError;
import static com.hazelcast.simulator.utils.CommonUtils.getSimulatorVersion;
import static com.hazelcast.simulator.utils.FileUtils.ensureExistingDirectory;
import static com.hazelcast.simulator.utils.FileUtils.getSimulatorHome;
import static java.lang.String.format;

public class Agent {

    private static final Logger LOGGER = Logger.getLogger(Coordinator.class);

    private final ConcurrentMap<String, WorkerJvm> workerJVMs = new ConcurrentHashMap<String, WorkerJvm>();
    private final WorkerJvmManager workerJvmManager = new WorkerJvmManager(this, workerJVMs);
    private final WorkerJvmFailureMonitor workerJvmFailureMonitor = new WorkerJvmFailureMonitor(this);

    private final int addressIndex;
    private final String publicAddress;

    private final String cloudProvider;
    private final String cloudIdentity;
    private final String cloudCredential;

    private final AgentRemoteService agentRemoteService;
    private final AgentConnector agentConnector;

    private final CoordinatorLogger coordinatorLogger;

    private volatile TestSuite testSuite;

    public Agent(int addressIndex, String publicAddress, String cloudProvider, String cloudIdentity, String cloudCredential) {
        ensureExistingDirectory(WorkerJvmManager.WORKERS_HOME);

        this.addressIndex = addressIndex;
        this.publicAddress = publicAddress;
        this.cloudProvider = cloudProvider;
        this.cloudIdentity = cloudIdentity;
        this.cloudCredential = cloudCredential;

        this.agentRemoteService = getAgentRemoteService();
        this.agentConnector = AgentConnector.createInstance(this, workerJVMs, Ports.AGENT_PORT);
        this.agentConnector.start();

        this.coordinatorLogger = new CoordinatorLogger(agentConnector);

        workerJvmFailureMonitor.start();
        workerJvmManager.start();

        LOGGER.info("Simulator Agent is ready for action!");
    }

    public int getAddressIndex() {
        return addressIndex;
    }

    public String getPublicAddress() {
        return publicAddress;
    }

    public AgentConnector getAgentConnector() {
        return agentConnector;
    }

    public CoordinatorLogger getCoordinatorLogger() {
        return coordinatorLogger;
    }

    public void initTestSuite(TestSuite testSuite) {
        this.testSuite = testSuite;

        File testSuiteDir = new File(WorkerJvmManager.WORKERS_HOME, testSuite.id);
        ensureExistingDirectory(testSuiteDir);

        File libDir = new File(testSuiteDir, "lib");
        ensureExistingDirectory(libDir);
    }

    public void echo(String msg) {
        LOGGER.info(msg);
    }

    public TestSuite getTestSuite() {
        return testSuite;
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

    void shutdown() {
        agentConnector.shutdown();
        try {
            agentRemoteService.shutdown();
        } catch (IOException e) {
            EmptyStatement.ignore(e);
        }
        workerJvmManager.shutdown();
    }

    private AgentRemoteService getAgentRemoteService() {
        try {
            AgentMessageProcessor messageProcessor = new AgentMessageProcessor(workerJvmManager);
            AgentRemoteService remoteService = new AgentRemoteService(this, messageProcessor);
            remoteService.start();
            return remoteService;
        } catch (Exception e) {
            throw new CommandLineExitException("Failed to start REST server", e);
        }
    }

    public static void main(String[] args) {
        try {
            createAgent(args);
        } catch (Exception e) {
            exitWithError(LOGGER, "Could not start agent!", e);
        }
    }

    static Agent createAgent(String[] args) {
        LOGGER.info("Simulator Agent");
        LOGGER.info(format("Version: %s, Commit: %s, Build Time: %s",
                getSimulatorVersion(),
                getCommitIdAbbrev(),
                getBuildTime()));
        LOGGER.info(format("SIMULATOR_HOME: %s%n", getSimulatorHome()));
        logInterestingSystemProperties();

        Agent agent = AgentCli.init(args);

        LOGGER.info("CloudIdentity: " + agent.cloudIdentity);
        LOGGER.info("CloudCredential: " + agent.cloudCredential);
        LOGGER.info("CloudProvider: " + agent.cloudProvider);

        return agent;
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
