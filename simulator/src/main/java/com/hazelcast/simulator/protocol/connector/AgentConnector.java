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
package com.hazelcast.simulator.protocol.connector;

import com.hazelcast.simulator.agent.Agent;
import com.hazelcast.simulator.agent.workerjvm.WorkerJvmManager;
import com.hazelcast.simulator.protocol.configuration.AgentServerConfiguration;
import com.hazelcast.simulator.protocol.configuration.ClientConfiguration;
import com.hazelcast.simulator.protocol.core.ConnectionManager;
import com.hazelcast.simulator.protocol.core.ResponseFuture;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.exception.ExceptionType;
import com.hazelcast.simulator.protocol.exception.RemoteExceptionLogger;
import com.hazelcast.simulator.protocol.processors.AgentOperationProcessor;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.simulator.protocol.core.AddressLevel.AGENT;

/**
 * Connector which listens for incoming Simulator Coordinator connections and manages Simulator Worker instances.
 */
public final class AgentConnector extends AbstractServerConnector {

    private final AgentServerConfiguration serverConfiguration;

    private AgentConnector(AgentServerConfiguration configuration) {
        super(configuration);
        this.serverConfiguration = configuration;
    }

    /**
     * Creates an {@link AgentConnector} instance.
     *
     * @param agent            instance of this Simulator Agent
     * @param workerJvmManager manager for WorkerJVM instances
     * @param port             the port for incoming connections
     */
    public static AgentConnector createInstance(Agent agent, WorkerJvmManager workerJvmManager, int port) {
        SimulatorAddress localAddress = new SimulatorAddress(AGENT, agent.getAddressIndex(), 0, 0);

        RemoteExceptionLogger exceptionLogger = new RemoteExceptionLogger(localAddress, ExceptionType.AGENT_EXCEPTION);
        AgentOperationProcessor processor = new AgentOperationProcessor(exceptionLogger, agent, workerJvmManager);
        ConcurrentMap<String, ResponseFuture> futureMap = new ConcurrentHashMap<String, ResponseFuture>();
        ConnectionManager connectionManager = new ConnectionManager();

        AgentServerConfiguration configuration = new AgentServerConfiguration(processor, futureMap, connectionManager,
                workerJvmManager, localAddress, port);

        AgentConnector connector = new AgentConnector(configuration);

        exceptionLogger.setServerConnector(connector);
        return connector;
    }

    /**
     * Adds a Simulator Worker and connects to it.
     *
     * @param workerIndex the index of the Simulator Worker
     * @param workerHost  the host of the Simulator Worker
     * @param workerPort  the port of the Simulator Worker
     * @return the {@link SimulatorAddress} of the Simulator Worker
     */
    public SimulatorAddress addWorker(int workerIndex, String workerHost, int workerPort) {
        ClientConfiguration clientConfiguration = serverConfiguration.getClientConfiguration(workerIndex, workerHost, workerPort,
                this);
        ClientConnector client = new ClientConnector(clientConfiguration);
        client.start();

        serverConfiguration.addWorker(workerIndex, client);

        return clientConfiguration.getRemoteAddress();
    }

    /**
     * Removes a Simulator Worker.
     *
     * @param workerIndex the index of the remote Simulator Worker
     */
    public void removeWorker(int workerIndex) {
        serverConfiguration.removeWorker(workerIndex);
    }
}
