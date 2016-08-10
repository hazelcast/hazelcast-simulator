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
package com.hazelcast.simulator.coordinator;

import com.hazelcast.simulator.protocol.connector.CoordinatorConnector;
import com.hazelcast.simulator.protocol.core.Response;
import com.hazelcast.simulator.protocol.core.ResponseType;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.core.SimulatorProtocolException;
import com.hazelcast.simulator.protocol.operation.LogOperation;
import com.hazelcast.simulator.protocol.operation.PingOperation;
import com.hazelcast.simulator.protocol.operation.SimulatorOperation;
import com.hazelcast.simulator.protocol.operation.StopTimeoutDetectionOperation;
import com.hazelcast.simulator.protocol.operation.TerminateWorkerOperation;
import com.hazelcast.simulator.protocol.registry.ComponentRegistry;
import com.hazelcast.simulator.utils.CommandLineExitException;
import org.apache.log4j.Logger;

import java.util.Map;

import static com.hazelcast.simulator.protocol.core.SimulatorAddress.ALL_AGENTS;
import static com.hazelcast.simulator.protocol.core.SimulatorAddress.ALL_WORKERS;
import static com.hazelcast.simulator.utils.CommonUtils.joinThread;
import static com.hazelcast.simulator.utils.CommonUtils.sleepMillis;
import static java.lang.String.format;

// TODO: The remote client needs to be dumbed down. It should just ship operations and have no other knowlege.
// pinging, shutdown etc.. Is not part of its responsibilities.
public class RemoteClient {

    private static final Logger LOGGER = Logger.getLogger(RemoteClient.class);

    private final CoordinatorConnector coordinatorConnector;
    private final ComponentRegistry componentRegistry;
    private final WorkerPingThread workerPingThread;
    private final int memberWorkerShutdownDelaySeconds;

    public RemoteClient(CoordinatorConnector coordinatorConnector,
                        ComponentRegistry componentRegistry,
                        int workerPingIntervalMillis,
                        int memberWorkerShutdownDelaySeconds) {
        this.coordinatorConnector = coordinatorConnector;
        this.componentRegistry = componentRegistry;
        this.workerPingThread = new WorkerPingThread(workerPingIntervalMillis);
        this.memberWorkerShutdownDelaySeconds = memberWorkerShutdownDelaySeconds;
    }

    public CoordinatorConnector getCoordinatorConnector() {
        return coordinatorConnector;
    }

    public void logOnAllAgents(String message) {
        coordinatorConnector.write(ALL_AGENTS, new LogOperation(message));
    }

    public void logOnAllWorkers(String message) {
        coordinatorConnector.write(ALL_WORKERS, new LogOperation(message));
    }

    void startWorkerPingThread() {
        if (workerPingThread.pingIntervalMillis > 0) {
            workerPingThread.start();
        }
    }

    void stopWorkerPingThread() {
        workerPingThread.running = false;
        workerPingThread.interrupt();
        joinThread(workerPingThread);
    }

    public void terminateWorkers(boolean stopPokeThread) {
        if (stopPokeThread) {
            sendToAllAgents(new StopTimeoutDetectionOperation());

            stopWorkerPingThread();
        }

        int shutdownDelaySeconds = (componentRegistry.hasClientWorkers() ? memberWorkerShutdownDelaySeconds : 0);
        sendToAllWorkers(new TerminateWorkerOperation(shutdownDelaySeconds, true));
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
            if (responseType != ResponseType.SUCCESS && responseType != ResponseType.UNBLOCKED_BY_FAILURE) {
                SimulatorAddress source = responseTypeEntry.getKey();
                throw new CommandLineExitException(format("Could not execute %s on %s (%s)", operation, source, responseType));
            }
        }
    }

    private final class WorkerPingThread extends Thread {

        private final int pingIntervalMillis;

        private volatile boolean running = true;

        private WorkerPingThread(int pingIntervalMillis) {
            super("WorkerPingThread");
            this.pingIntervalMillis = pingIntervalMillis;

            setDaemon(true);
        }

        @Override
        public void run() {
            PingOperation operation = new PingOperation();
            while (running) {
                try {
                    sendToAllWorkers(operation);
                    sleepMillis(pingIntervalMillis);
                } catch (SimulatorProtocolException e) {
                    if (e.getCause() instanceof InterruptedException) {
                        break;
                    }
                    LOGGER.error("Exception in WorkerPingThread", e);
                }
            }
        }
    }
}
