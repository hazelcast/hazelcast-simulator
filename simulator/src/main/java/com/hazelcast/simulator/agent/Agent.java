/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.simulator.agent.workerprocess.WorkerProcessFailureMonitor;
import com.hazelcast.simulator.agent.workerprocess.WorkerProcessManager;
import com.hazelcast.simulator.common.ShutdownThread;
import com.hazelcast.simulator.protocol.connector.AgentConnector;
import com.hazelcast.simulator.protocol.connector.AgentConnectorImpl;
import com.hazelcast.simulator.protocol.operation.OperationTypeCounter;
import org.apache.log4j.Logger;

import java.io.Closeable;
import java.io.File;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.simulator.utils.FileUtils.deleteQuiet;
import static com.hazelcast.simulator.utils.FileUtils.ensureExistingDirectory;
import static com.hazelcast.simulator.utils.FileUtils.getSimulatorHome;
import static com.hazelcast.simulator.utils.FileUtils.getUserDir;
import static com.hazelcast.simulator.utils.FileUtils.writeText;
import static com.hazelcast.simulator.utils.NativeUtils.getPID;

public class Agent implements Closeable {

    private static final Logger LOGGER = Logger.getLogger(Agent.class);

    private final AtomicBoolean shutdownStarted = new AtomicBoolean();
    private final WorkerProcessManager workerProcessManager = new WorkerProcessManager();
    private final int addressIndex;
    private final String publicAddress;
    private final int port;
    private final File pidFile = new File(getUserDir(), "agent.pid");

    private final AgentConnector agentConnector;
    private final WorkerProcessFailureHandlerImpl failureSender;
    private final WorkerProcessFailureMonitor workerProcessFailureMonitor;

    private volatile String sessionId;

    public Agent(int addressIndex,
                 String publicAddress,
                 int port,
                 int threadPoolSize,
                 int workerLastSeenTimeoutSeconds) {
        shutdownStarted.set(false);

        this.addressIndex = addressIndex;
        this.publicAddress = publicAddress;
        this.port = port;
        this.agentConnector = new AgentConnectorImpl(this, workerProcessManager, port, threadPoolSize);
        this.failureSender = new WorkerProcessFailureHandlerImpl(publicAddress, agentConnector);
        this.workerProcessFailureMonitor = new WorkerProcessFailureMonitor(failureSender, workerProcessManager,
                workerLastSeenTimeoutSeconds);

        Runtime.getRuntime().addShutdownHook(new AgentShutdownThread(true));

        createPidFile();
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

    public WorkerProcessFailureMonitor getWorkerProcessFailureMonitor() {
        return workerProcessFailureMonitor;
    }

    public WorkerProcessManager getWorkerProcessManager() {
        return workerProcessManager;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    public File getSessionDirectory() {
        String sessionId = this.sessionId;
        if (sessionId == null) {
            throw new IllegalStateException("no session active");
        }

        File workersDir = ensureExistingDirectory(getSimulatorHome(), "workers");
        return ensureExistingDirectory(workersDir, sessionId);
    }

    public void start() {
        LOGGER.info("Agent starting...");

        agentConnector.start();
        workerProcessFailureMonitor.start();

        LOGGER.info("Agent started!");
    }

    @Override
    public void close() {
        ShutdownThread thread = new AgentShutdownThread(false);
        thread.start();
        thread.awaitShutdown();
    }

    public String getSessionId() {
        return sessionId;
    }

    private final class AgentShutdownThread extends ShutdownThread {

        private AgentShutdownThread(boolean ensureProcessShutdown) {
            super("AgentShutdownThread", shutdownStarted, ensureProcessShutdown);
        }

        @Override
        public void doRun() {
            LOGGER.info("Stopping workers...");
            workerProcessManager.shutdown();

            LOGGER.info("Stopping WorkerProcessFailureMonitor...");
            workerProcessFailureMonitor.shutdown();

            LOGGER.info("Stopping AgentConnector...");
            agentConnector.close();

            LOGGER.info("Removing PID file...");
            deleteQuiet(pidFile);

            OperationTypeCounter.printStatistics();
        }
    }
}
