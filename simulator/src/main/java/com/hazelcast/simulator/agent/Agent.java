/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.simulator.agent.workerjvm.WorkerJvmFailureMonitor;
import com.hazelcast.simulator.agent.workerjvm.WorkerJvmManager;
import com.hazelcast.simulator.common.CoordinatorLogger;
import com.hazelcast.simulator.common.ShutdownThread;
import com.hazelcast.simulator.protocol.connector.AgentConnector;
import com.hazelcast.simulator.protocol.operation.OperationTypeCounter;
import com.hazelcast.simulator.test.TestSuite;
import org.apache.log4j.Logger;

import java.io.File;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.simulator.common.GitInfo.getBuildTime;
import static com.hazelcast.simulator.common.GitInfo.getCommitIdAbbrev;
import static com.hazelcast.simulator.utils.CommonUtils.exitWithError;
import static com.hazelcast.simulator.utils.CommonUtils.getSimulatorVersion;
import static com.hazelcast.simulator.utils.FileUtils.deleteQuiet;
import static com.hazelcast.simulator.utils.FileUtils.ensureExistingDirectory;
import static com.hazelcast.simulator.utils.FileUtils.getSimulatorHome;
import static com.hazelcast.simulator.utils.FileUtils.writeText;
import static com.hazelcast.simulator.utils.NativeUtils.getPID;
import static java.lang.String.format;

public class Agent {

    private static final Logger LOGGER = Logger.getLogger(Agent.class);
    private static final AtomicBoolean SHUTDOWN_STARTED = new AtomicBoolean();

    private final File pidFile = new File("agent.pid");

    private final WorkerJvmManager workerJvmManager = new WorkerJvmManager();

    private final int addressIndex;
    private final String publicAddress;
    private final int port;

    private final String cloudProvider;
    private final String cloudIdentity;
    private final String cloudCredential;

    private final WorkerJvmFailureMonitor workerJvmFailureMonitor;
    private final AgentConnector agentConnector;
    private final CoordinatorLogger coordinatorLogger;

    private volatile TestSuite testSuite;

    public Agent(int addressIndex, String publicAddress, int port, String cloudProvider, String cloudIdentity,
                 String cloudCredential, int threadPoolSize, int workerLastSeenTimeoutSeconds) {
        SHUTDOWN_STARTED.set(false);

        this.addressIndex = addressIndex;
        this.publicAddress = publicAddress;
        this.port = port;

        this.cloudProvider = cloudProvider;
        this.cloudIdentity = cloudIdentity;
        this.cloudCredential = cloudCredential;

        this.workerJvmFailureMonitor = new WorkerJvmFailureMonitor(this, workerJvmManager, workerLastSeenTimeoutSeconds);

        this.agentConnector = AgentConnector.createInstance(this, workerJvmManager, port, threadPoolSize);
        this.agentConnector.start();

        this.coordinatorLogger = new CoordinatorLogger(agentConnector);

        Runtime.getRuntime().addShutdownHook(new AgentShutdownThread(true));

        createPidFile();

        LOGGER.info("Simulator Agent is ready for action!");
    }

    private void createPidFile() {
        deleteQuiet(pidFile);
        writeText("" + getPID(), pidFile);
    }

    public int getAddressIndex() {
        return addressIndex;
    }

    public String getPublicAddress() {
        return publicAddress;
    }

    public int getPort() {
        return port;
    }

    public AgentConnector getAgentConnector() {
        return agentConnector;
    }

    public CoordinatorLogger getCoordinatorLogger() {
        return coordinatorLogger;
    }

    public WorkerJvmFailureMonitor getWorkerJvmFailureMonitor() {
        return workerJvmFailureMonitor;
    }

    public void setTestSuite(TestSuite testSuite) {
        this.testSuite = testSuite;
    }

    public TestSuite getTestSuite() {
        return testSuite;
    }

    public File getTestSuiteDir() {
        if (testSuite == null) {
            return null;
        }

        File workersDir = ensureExistingDirectory(getSimulatorHome(), "workers");
        return new File(workersDir, testSuite.getId());
    }

    void shutdown() throws Exception {
        ShutdownThread thread = new AgentShutdownThread(false);
        thread.start();
        thread.awaitShutdown();
    }

    public static void main(String[] args) {
        try {
            createAgent(args);
        } catch (Exception e) {
            exitWithError(LOGGER, "Could not start Agent!", e);
        }
    }

    static Agent createAgent(String[] args) {
        LOGGER.info("Simulator Agent");
        LOGGER.info(format("Version: %s, Commit: %s, Build Time: %s",
                getSimulatorVersion(),
                getCommitIdAbbrev(),
                getBuildTime()));
        LOGGER.info(format("SIMULATOR_HOME: %s%n", getSimulatorHome()));
        logImportantSystemProperties();

        Agent agent = AgentCli.init(args);

        LOGGER.info("CloudIdentity: " + agent.cloudIdentity);
        LOGGER.info("CloudCredential: " + agent.cloudCredential);
        LOGGER.info("CloudProvider: " + agent.cloudProvider);

        return agent;
    }

    private static void logImportantSystemProperties() {
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

    private final class AgentShutdownThread extends ShutdownThread {

        private AgentShutdownThread(boolean shutdownLog4j) {
            super("AgentShutdownThread", SHUTDOWN_STARTED, shutdownLog4j);
        }

        @Override
        public void doRun() {
            LOGGER.info("Stopping workers...");
            workerJvmManager.shutdown();

            LOGGER.info("Stopping WorkerJvmFailureMonitor...");
            workerJvmFailureMonitor.shutdown();

            LOGGER.info("Stopping AgentConnector...");
            agentConnector.shutdown();

            LOGGER.info("Removing PID file...");
            deleteQuiet(pidFile);

            OperationTypeCounter.printStatistics();
        }
    }
}
