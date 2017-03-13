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

import com.hazelcast.simulator.protocol.connector.Connector;
import com.hazelcast.simulator.protocol.core.Response;
import com.hazelcast.simulator.protocol.core.ResponseType;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.core.SimulatorProtocolException;
import com.hazelcast.simulator.protocol.operation.LogOperation;
import com.hazelcast.simulator.protocol.operation.PingOperation;
import com.hazelcast.simulator.protocol.operation.SimulatorOperation;
import com.hazelcast.simulator.coordinator.registry.ComponentRegistry;
import com.hazelcast.simulator.utils.CommandLineExitException;
import org.apache.log4j.Logger;

import java.io.Closeable;
import java.util.Map;

import static com.hazelcast.simulator.protocol.core.SimulatorAddress.ALL_AGENTS;
import static com.hazelcast.simulator.protocol.core.SimulatorAddress.ALL_WORKERS;
import static com.hazelcast.simulator.utils.CommonUtils.joinThread;
import static com.hazelcast.simulator.utils.CommonUtils.sleepMillis;
import static java.lang.String.format;

/**
 * Responsible for communication with Simulator Agents and Workers.
 */
public class RemoteClient implements Closeable {

    private static final Logger LOGGER = Logger.getLogger(RemoteClient.class);

    private final Connector connector;
    private final ComponentRegistry componentRegistry;
    private final WorkerPingThread workerPingThread;

    public RemoteClient(Connector connector,
                        ComponentRegistry componentRegistry,
                        int workerPingIntervalMillis) {
        this.connector = connector;
        this.componentRegistry = componentRegistry;
        this.workerPingThread = new WorkerPingThread(workerPingIntervalMillis);

        if (workerPingThread.pingIntervalMillis > 0) {
            workerPingThread.start();
        }
    }

    public Connector getConnector() {
        return connector;
    }

    public void logOnAllAgents(String message) {
        try {
            connector.invoke(ALL_AGENTS, new LogOperation(message));
        } catch (Exception e) {
            LOGGER.debug("Failed to log on all agents", e);
        }
    }

    public void logOnAllWorkers(String message) {
        try {
            connector.invoke(ALL_WORKERS, new LogOperation(message));
        } catch (Exception e) {
            LOGGER.debug("Failed to log on all workers", e);
        }
    }

    public void invokeOnAllAgents(SimulatorOperation operation) {
        Response response = connector.invoke(ALL_AGENTS, operation);
        validateResponse(operation, response);
    }

    public void invokeOnAllWorkers(SimulatorOperation operation) {
        Response response = connector.invoke(ALL_WORKERS, operation);
        validateResponse(operation, response);
    }

    public void invokeOnFirstWorker(SimulatorOperation operation) {
        Response response = connector.invoke(componentRegistry.getFirstWorker().getAddress(), operation);
        validateResponse(operation, response);
    }

    public void invokeOnTestOnAllWorkers(SimulatorAddress testAddress, SimulatorOperation operation) {
        Response response = connector.invoke(testAddress, operation);
        validateResponse(operation, response);
    }

    public void invokeOnTestOnFirstWorker(SimulatorAddress testAddress, SimulatorOperation operation) {
        SimulatorAddress firstWorkerAddress = componentRegistry.getFirstWorker().getAddress();
        Response response = connector.invoke(firstWorkerAddress.getChild(testAddress.getTestIndex()), operation);
        validateResponse(operation, response);
    }

    private void validateResponse(SimulatorOperation operation, Response response) {
        for (Map.Entry<SimulatorAddress, Response.Part> entry : response.getParts()) {
            ResponseType responseType = entry.getValue().getType();
            if (responseType != ResponseType.SUCCESS && responseType != ResponseType.UNBLOCKED_BY_FAILURE) {
                SimulatorAddress source = entry.getKey();
                throw new CommandLineExitException(format("Could not execute %s on %s (%s)", operation, source, responseType));
            }
        }
    }

    @Override
    public void close() {
        workerPingThread.running = false;
        workerPingThread.interrupt();
        joinThread(workerPingThread);
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
                    invokeOnAllWorkers(operation);
                    sleepMillis(pingIntervalMillis);
                } catch (SimulatorProtocolException e) {
                    if (e.getCause() instanceof InterruptedException) {
                        break;
                    }
                }
            }
        }
    }
}
