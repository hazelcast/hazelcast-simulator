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
package com.hazelcast.simulator.protocol.connector;

import com.hazelcast.simulator.coordinator.FailureContainer;
import com.hazelcast.simulator.coordinator.FailureListener;
import com.hazelcast.simulator.coordinator.PerformanceStatsContainer;
import com.hazelcast.simulator.coordinator.TestPhaseListeners;
import com.hazelcast.simulator.protocol.core.ConnectionManager;
import com.hazelcast.simulator.protocol.core.Response;
import com.hazelcast.simulator.protocol.core.ResponseFuture;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.core.SimulatorMessage;
import com.hazelcast.simulator.protocol.core.SimulatorProtocolException;
import com.hazelcast.simulator.protocol.exception.LocalExceptionLogger;
import com.hazelcast.simulator.protocol.handler.ConnectionListenerHandler;
import com.hazelcast.simulator.protocol.handler.ConnectionValidationHandler;
import com.hazelcast.simulator.protocol.handler.ExceptionHandler;
import com.hazelcast.simulator.protocol.handler.MessageConsumeHandler;
import com.hazelcast.simulator.protocol.handler.MessageEncoder;
import com.hazelcast.simulator.protocol.handler.ResponseEncoder;
import com.hazelcast.simulator.protocol.handler.ResponseHandler;
import com.hazelcast.simulator.protocol.handler.SimulatorFrameDecoder;
import com.hazelcast.simulator.protocol.handler.SimulatorProtocolDecoder;
import com.hazelcast.simulator.protocol.operation.FailureOperation;
import com.hazelcast.simulator.protocol.operation.SimulatorOperation;
import com.hazelcast.simulator.protocol.processors.CoordinatorCommunicatorProcessor;
import com.hazelcast.simulator.protocol.processors.CoordinatorOperationProcessor;
import com.hazelcast.simulator.protocol.registry.ComponentRegistry;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.group.ChannelGroup;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.simulator.protocol.core.ResponseType.FAILURE_AGENT_NOT_FOUND;
import static com.hazelcast.simulator.protocol.core.SimulatorAddress.COORDINATOR;
import static com.hazelcast.simulator.protocol.core.SimulatorAddress.REMOTE;
import static java.util.Collections.unmodifiableCollection;

/**
 * Connector which connects to remote Simulator Agent instances.
 */
@SuppressWarnings("checkstyle:classdataabstractioncoupling")
public class CoordinatorConnector extends AbstractServerConnector implements ClientPipelineConfigurator, FailureListener {

    private final LocalExceptionLogger exceptionLogger = new LocalExceptionLogger();

    private final CoordinatorOperationProcessor processor;
    private final ConnectionManager connectionManager;

    CoordinatorConnector(FailureContainer failureContainer, TestPhaseListeners testPhaseListeners,
                         PerformanceStatsContainer performanceStatsContainer, int port,
                         ConnectionManager connectionManager, ConcurrentMap<String, ResponseFuture> futureMap,
                         ComponentRegistry componentRegistry) {
        super(futureMap, COORDINATOR, port, getDefaultThreadPoolSize());

        CoordinatorCommunicatorProcessor communicatorProcessor = new CoordinatorCommunicatorProcessor(this, componentRegistry);

        this.processor = new CoordinatorOperationProcessor(exceptionLogger, failureContainer, testPhaseListeners,
                performanceStatsContainer, communicatorProcessor);
        this.connectionManager = connectionManager;
    }

    /**
     * Creates a {@link CoordinatorConnector} instance.
     *
     * @param componentRegistry         {@link ComponentRegistry} for this connector
     * @param failureContainer          {@link FailureContainer} for this connector
     * @param testPhaseListeners        {@link TestPhaseListeners} for this connector
     * @param performanceStatsContainer {@link PerformanceStatsContainer} for this connector
     * @param port                      the port for incoming connections
     */
    public static CoordinatorConnector createInstance(ComponentRegistry componentRegistry,
                                                      FailureContainer failureContainer,
                                                      TestPhaseListeners testPhaseListeners,
                                                      PerformanceStatsContainer performanceStatsContainer, int port) {
        ConnectionManager connectionManager = new ConnectionManager();
        ConcurrentHashMap<String, ResponseFuture> futureMap = new ConcurrentHashMap<String, ResponseFuture>();

        return new CoordinatorConnector(failureContainer, testPhaseListeners, performanceStatsContainer,
                port, connectionManager, futureMap, componentRegistry);
    }

    @Override
    public void onFailure(FailureOperation operation, boolean isFinishedFailure, boolean isCritical) {
        if (!isFinishedFailure) {
            return;
        }
        SimulatorAddress workerAddress = operation.getWorkerAddress();
        if (workerAddress == null) {
            return;
        }
        for (ResponseFuture future : getFutureMap().values()) {
            future.unblockOnFailure(workerAddress, COORDINATOR, workerAddress.getAgentIndex());
        }
    }

    @Override
    public void configureClientPipeline(ChannelPipeline pipeline, SimulatorAddress remoteAddress,
                                        ConcurrentMap<String, ResponseFuture> futureMap) {
        pipeline.addLast("responseEncoder", new ResponseEncoder(COORDINATOR));
        pipeline.addLast("messageEncoder", new MessageEncoder(COORDINATOR, remoteAddress));
        pipeline.addLast("frameDecoder", new SimulatorFrameDecoder());
        pipeline.addLast("protocolDecoder", new SimulatorProtocolDecoder(COORDINATOR));
        pipeline.addLast("responseHandler", new ResponseHandler(COORDINATOR, remoteAddress, futureMap));
        pipeline.addLast("messageConsumeHandler", new MessageConsumeHandler(COORDINATOR, processor, getScheduledExecutor()));
        pipeline.addLast("exceptionHandler", new ExceptionHandler(this));
    }

    @Override
    void configureServerPipeline(ChannelPipeline pipeline, ServerConnector serverConnector) {
        pipeline.addLast("connectionValidationHandler", new ConnectionValidationHandler());
        pipeline.addLast("connectionListenerHandler", new ConnectionListenerHandler(connectionManager));
        pipeline.addLast("responseEncoder", new ResponseEncoder(COORDINATOR));
        pipeline.addLast("messageEncoder", new MessageEncoder(COORDINATOR, COORDINATOR));
        pipeline.addLast("frameDecoder", new SimulatorFrameDecoder());
        pipeline.addLast("protocolDecoder", new SimulatorProtocolDecoder(COORDINATOR));
        pipeline.addLast("messageConsumeHandler", new MessageConsumeHandler(COORDINATOR, processor,
                getScheduledExecutor()));
        pipeline.addLast("responseHandler", new ResponseHandler(COORDINATOR, REMOTE, getFutureMap(), 0));
        pipeline.addLast("exceptionHandler", new ExceptionHandler(this));
    }

    @Override
    ChannelGroup getChannelGroup() {
        return connectionManager.getChannels();
    }

    /**
     * Adds a Simulator Agent and connects to it.
     *
     * @param agentIndex the index of the Simulator Agent
     * @param agentHost  the host of the Simulator Agent
     * @param agentPort  the port of the Simulator Agent
     */
    public void addAgent(int agentIndex, String agentHost, int agentPort) {
        ClientConnector client = new ClientConnector(this, getEventLoopGroup(), getFutureMap(), COORDINATOR,
                COORDINATOR.getChild(agentIndex), agentIndex, agentHost, agentPort);
        client.start();

        getClientConnectorManager().addClient(agentIndex, client);
    }

    /**
     * Removes a Simulator Agent.
     *
     * @param agentIndex the index of the remote Simulator Agent
     */
    public void removeAgent(int agentIndex) {
        getClientConnectorManager().removeClient(agentIndex);
    }

    /**
     * Sends a {@link SimulatorMessage} to the addressed Simulator component.
     *
     * @param destination the {@link SimulatorAddress} of the destination
     * @param operation   the {@link SimulatorOperation} to send
     * @return a {@link Response} with the response of all addressed Simulator components.
     */
    @Override
    public Response write(SimulatorAddress destination, SimulatorOperation operation) {
        SimulatorMessage message = createSimulatorMessage(COORDINATOR, destination, operation);

        int agentAddressIndex = destination.getAgentIndex();
        Response response = new Response(message);
        List<ResponseFuture> futureList = new ArrayList<ResponseFuture>();
        if (agentAddressIndex == 0) {
            for (ClientConnector agent : getClientConnectorManager().getClientConnectors()) {
                futureList.add(agent.writeAsync(message));
            }
        } else {
            ClientConnector agent = getClientConnectorManager().get(agentAddressIndex);
            if (agent == null) {
                response.addResponse(COORDINATOR, FAILURE_AGENT_NOT_FOUND);
            } else {
                futureList.add(agent.writeAsync(message));
            }
        }
        try {
            for (ResponseFuture future : futureList) {
                response.addResponse(future.get());
            }
        } catch (InterruptedException e) {
            throw new SimulatorProtocolException("ResponseFuture.get() got interrupted!", e);
        }
        return response;
    }

    public Response writeToCommunicator(SimulatorOperation operation) {
        return super.write(REMOTE, operation);
    }

    /**
     * Returns the number of collected exceptions.
     *
     * @return the number of exceptions.
     */
    public int getExceptionCount() {
        return exceptionLogger.getExceptionCount();
    }

    // just for testing
    public Collection<ClientConnector> getClientConnectors() {
        return unmodifiableCollection(getClientConnectorManager().getClientConnectors());
    }

    // just for testing
    void addAgent(int agentIndex, ClientConnector agent) {
        getClientConnectorManager().addClient(agentIndex, agent);
    }
}
