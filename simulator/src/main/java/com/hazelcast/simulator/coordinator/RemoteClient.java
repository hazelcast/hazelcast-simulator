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
package com.hazelcast.simulator.coordinator;

import com.hazelcast.simulator.agent.workerjvm.WorkerJvmSettings;
import com.hazelcast.simulator.cluster.AgentWorkerLayout;
import com.hazelcast.simulator.cluster.ClusterLayout;
import com.hazelcast.simulator.protocol.connector.CoordinatorConnector;
import com.hazelcast.simulator.protocol.core.Response;
import com.hazelcast.simulator.protocol.core.ResponseType;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.core.SimulatorProtocolException;
import com.hazelcast.simulator.protocol.operation.CreateWorkerOperation;
import com.hazelcast.simulator.protocol.operation.InitTestSuiteOperation;
import com.hazelcast.simulator.protocol.operation.LogOperation;
import com.hazelcast.simulator.protocol.operation.SimulatorOperation;
import com.hazelcast.simulator.protocol.operation.StartTimeoutDetectionOperation;
import com.hazelcast.simulator.protocol.operation.StopTimeoutDetectionOperation;
import com.hazelcast.simulator.protocol.operation.TerminateWorkerOperation;
import com.hazelcast.simulator.protocol.registry.ComponentRegistry;
import com.hazelcast.simulator.test.TestSuite;
import com.hazelcast.simulator.utils.CommandLineExitException;
import com.hazelcast.simulator.utils.ThreadSpawner;
import com.hazelcast.simulator.worker.WorkerType;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.hazelcast.simulator.protocol.core.SimulatorAddress.ALL_AGENTS;
import static com.hazelcast.simulator.protocol.core.SimulatorAddress.ALL_WORKERS;
import static com.hazelcast.simulator.utils.CommonUtils.joinThread;
import static com.hazelcast.simulator.utils.CommonUtils.sleepSeconds;
import static java.lang.String.format;

public class RemoteClient {

    private static final int WORKER_POKE_INTERVAL_SECONDS = 10;

    private static final Logger LOGGER = Logger.getLogger(RemoteClient.class);

    private final WorkerPokeThread workerPokeThread = new WorkerPokeThread();

    private final CoordinatorConnector coordinatorConnector;
    private final ComponentRegistry componentRegistry;

    public RemoteClient(CoordinatorConnector coordinatorConnector, ComponentRegistry componentRegistry) {
        this.coordinatorConnector = coordinatorConnector;
        this.componentRegistry = componentRegistry;
    }

    public void logOnAllAgents(String message) {
        coordinatorConnector.write(ALL_AGENTS, new LogOperation(message));
    }

    public void logOnAllWorkers(String message) {
        coordinatorConnector.write(ALL_WORKERS, new LogOperation(message));
    }

    public void createWorkers(ClusterLayout clusterLayout, boolean startPokeThread) {
        createWorkersByType(clusterLayout, true);
        createWorkersByType(clusterLayout, false);

        sendToAllAgents(new StartTimeoutDetectionOperation());
        if (startPokeThread) {
            workerPokeThread.start();
        }
    }

    private void createWorkersByType(ClusterLayout clusterLayout, boolean isMemberType) {
        ThreadSpawner spawner = new ThreadSpawner("createWorkers", true);
        for (AgentWorkerLayout agentWorkerLayout : clusterLayout.getAgentWorkerLayouts()) {
            final List<WorkerJvmSettings> settingsList = new ArrayList<WorkerJvmSettings>();
            for (WorkerJvmSettings workerJvmSettings : agentWorkerLayout.getWorkerJvmSettings()) {
                WorkerType workerType = workerJvmSettings.getWorkerType();
                if (workerType.isMember() == isMemberType) {
                    settingsList.add(workerJvmSettings);
                }
            }

            final int workerCount = settingsList.size();
            if (workerCount == 0) {
                continue;
            }
            final SimulatorAddress agentAddress = agentWorkerLayout.getSimulatorAddress();
            final String workerType = (isMemberType) ? "member" : "client";
            spawner.spawn(new Runnable() {
                @Override
                public void run() {
                    CreateWorkerOperation operation = new CreateWorkerOperation(settingsList);
                    Response response = coordinatorConnector.write(agentAddress, operation);

                    ResponseType responseType = response.getFirstErrorResponseType();
                    if (responseType != ResponseType.SUCCESS) {
                        throw new CommandLineExitException(format("Could not create %d %s Worker on %s (%s)",
                                workerCount, workerType, agentAddress, responseType));
                    }

                    LOGGER.info(format("Created %d %s Worker on %s", workerCount, workerType, agentAddress));
                    componentRegistry.addWorkers(agentAddress, settingsList);
                }
            });
        }
        spawner.awaitCompletion();
    }

    public void terminateWorkers(boolean stopPokeThread) {
        if (stopPokeThread) {
            sendToAllAgents(new StopTimeoutDetectionOperation());

            workerPokeThread.running = false;
            workerPokeThread.interrupt();
            joinThread(workerPokeThread);
        }

        sendToAllWorkers(new TerminateWorkerOperation());
    }

    public void initTestSuite(TestSuite testSuite) {
        sendToAllAgents(new InitTestSuiteOperation(testSuite));
    }

    public void sendToAllAgents(SimulatorOperation operation) {
        Response response = coordinatorConnector.write(ALL_AGENTS, operation);
        validateResponse(operation, response);
    }

    public void sendToAllWorkers(SimulatorOperation operation) {
        Response response = coordinatorConnector.write(ALL_WORKERS, operation);
        validateResponse(operation, response);
    }

    public void sendToFirstWorker(SimulatorOperation operation) {
        Response response = coordinatorConnector.write(componentRegistry.getFirstWorker().getAddress(), operation);
        validateResponse(operation, response);
    }

    public void sendToTestOnAllWorkers(String testId, SimulatorOperation operation) {
        Response response = coordinatorConnector.write(componentRegistry.getTest(testId).getAddress(), operation);
        validateResponse(operation, response);
    }

    public void sendToTestOnFirstWorker(String testId, SimulatorOperation operation) {
        SimulatorAddress firstWorkerAddress = componentRegistry.getFirstWorker().getAddress();
        SimulatorAddress testAddress = componentRegistry.getTest(testId).getAddress();
        Response response = coordinatorConnector.write(firstWorkerAddress.getChild(testAddress.getTestIndex()), operation);
        validateResponse(operation, response);
    }

    private void validateResponse(SimulatorOperation operation, Response response) {
        for (Map.Entry<SimulatorAddress, ResponseType> responseTypeEntry : response.entrySet()) {
            ResponseType responseType = responseTypeEntry.getValue();
            if (responseType != ResponseType.SUCCESS) {
                SimulatorAddress source = responseTypeEntry.getKey();
                throw new CommandLineExitException(format("Could not execute %s on %s (%s)", operation, source, responseType));
            }
        }
    }

    private class WorkerPokeThread extends Thread {

        private volatile boolean running = true;

        public WorkerPokeThread() {
            super("WorkerPokeThread");
            setDaemon(true);
        }

        @Override
        public void run() {
            while (running) {
                try {
                    logOnAllWorkers("Poked by Coordinator...");
                    sleepSeconds(WORKER_POKE_INTERVAL_SECONDS);
                } catch (SimulatorProtocolException e) {
                    if (e.getCause() instanceof InterruptedException) {
                        break;
                    }
                    throw e;
                }
            }
        }
    }
}
