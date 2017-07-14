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

import com.hazelcast.simulator.agent.workerprocess.WorkerProcessFailureHandler;
import com.hazelcast.simulator.agent.workerprocess.WorkerProcessFailureMonitor;
import com.hazelcast.simulator.agent.workerprocess.WorkerProcessManager;
import com.hazelcast.simulator.common.ShutdownThread;
import com.hazelcast.simulator.protocol.Broker;
import com.hazelcast.simulator.protocol.Server;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.worker.ExitingExceptionListener;
import org.apache.log4j.Logger;

import java.io.Closeable;
import java.io.File;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.simulator.protocol.core.SimulatorAddress.agentAddress;
import static com.hazelcast.simulator.utils.CommonUtils.closeQuietly;
import static com.hazelcast.simulator.utils.FileUtils.deleteQuiet;
import static com.hazelcast.simulator.utils.FileUtils.getUserDir;
import static com.hazelcast.simulator.utils.NativeUtils.writePid;
import static com.hazelcast.simulator.utils.SimulatorUtils.localIp;

public class Agent implements Closeable {

    private static final Logger LOGGER = Logger.getLogger(Agent.class);

    private final AtomicBoolean shutdownStarted = new AtomicBoolean();
    private final WorkerProcessManager processManager;
    private final String publicAddress;
    private final File pidFile = new File(getUserDir(), "agent.pid");

    private final WorkerProcessFailureMonitor workerProcessFailureMonitor;
    private final Server server;
    private final Broker broker;
    private final WorkerSniffer workerSniffer;

    public Agent(int addressIndex,
                 String publicAddress,
                 int port,
                 int workerLastSeenTimeoutSeconds) {
        SimulatorAddress agentAddress = agentAddress(addressIndex);

        this.publicAddress = publicAddress;

        this.broker = new Broker()
                .setBrokerAddress(localIp(), port);

        // this server will listen to requests on the 'agents' topic
        this.server = new Server("agents")
                .setExceptionListener(new ExitingExceptionListener())
                .setSelfAddress(agentAddress);

        this.processManager = new WorkerProcessManager(server, agentAddress, publicAddress);

        this.workerSniffer = new WorkerSniffer(processManager);

        this.workerProcessFailureMonitor = new WorkerProcessFailureMonitor(
                new WorkerProcessFailureHandler(publicAddress, server),
                processManager, workerLastSeenTimeoutSeconds);

        server.setProcessor(new AgentOperationProcessor(processManager, workerProcessFailureMonitor));

        Runtime.getRuntime().addShutdownHook(new AgentShutdownThread(true));
    }


    public String getPublicAddress() {
        return publicAddress;
    }

    public WorkerProcessManager getProcessManager() {
        return processManager;
    }

    public void start() {
        LOGGER.info("Agent starting...");

        broker.start();

        server.setBrokerURL(broker.getBrokerURL())
                .start();

        workerSniffer.setConnection(server.getConnection())
                .start();

        workerProcessFailureMonitor.start();

        LOGGER.info("Agent started!");

        writePid(pidFile);
    }

    @Override
    public void close() {
        ShutdownThread thread = new AgentShutdownThread(false);
        thread.start();
        thread.awaitShutdown();
    }

    private final class AgentShutdownThread extends ShutdownThread {

        private AgentShutdownThread(boolean ensureProcessShutdown) {
            super("AgentShutdownThread", shutdownStarted, ensureProcessShutdown);
        }

        @Override
        public void doRun() {
            LOGGER.info("Stopping workers...");
            processManager.shutdown();

            LOGGER.info("Stopping WorkerProcessFailureMonitor...");
            workerProcessFailureMonitor.shutdown();

            workerSniffer.stop();
            closeQuietly(server);
            closeQuietly(broker);

            LOGGER.info("Removing PID file...");
            deleteQuiet(pidFile);
        }
    }
}
