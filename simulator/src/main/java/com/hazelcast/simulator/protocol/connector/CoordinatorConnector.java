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
import com.hazelcast.simulator.coordinator.PerformanceStateContainer;
import com.hazelcast.simulator.coordinator.HdrHistogramContainer;
import com.hazelcast.simulator.coordinator.TestPhaseListeners;
import com.hazelcast.simulator.protocol.core.ClientConnectorManager;
import com.hazelcast.simulator.protocol.core.Response;
import com.hazelcast.simulator.protocol.core.ResponseFuture;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.core.SimulatorMessage;
import com.hazelcast.simulator.protocol.core.SimulatorProtocolException;
import com.hazelcast.simulator.protocol.exception.LocalExceptionLogger;
import com.hazelcast.simulator.protocol.handler.MessageConsumeHandler;
import com.hazelcast.simulator.protocol.handler.MessageEncoder;
import com.hazelcast.simulator.protocol.handler.ResponseEncoder;
import com.hazelcast.simulator.protocol.handler.ResponseHandler;
import com.hazelcast.simulator.protocol.handler.SimulatorFrameDecoder;
import com.hazelcast.simulator.protocol.handler.SimulatorProtocolDecoder;
import com.hazelcast.simulator.protocol.operation.FailureOperation;
import com.hazelcast.simulator.protocol.operation.SimulatorOperation;
import com.hazelcast.simulator.protocol.processors.CoordinatorOperationProcessor;
import com.hazelcast.simulator.utils.ThreadSpawner;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.simulator.protocol.connector.ServerConnector.DEFAULT_SHUTDOWN_QUIET_PERIOD;
import static com.hazelcast.simulator.protocol.connector.ServerConnector.DEFAULT_SHUTDOWN_TIMEOUT;
import static com.hazelcast.simulator.protocol.core.ResponseType.FAILURE_AGENT_NOT_FOUND;
import static com.hazelcast.simulator.protocol.core.SimulatorAddress.COORDINATOR;
import static com.hazelcast.simulator.protocol.operation.OperationCodec.toJson;
import static com.hazelcast.simulator.protocol.operation.OperationType.getOperationType;
import static com.hazelcast.simulator.utils.CommonUtils.awaitTermination;
import static com.hazelcast.simulator.utils.ExecutorFactory.createFixedThreadPool;
import static java.util.Collections.unmodifiableCollection;

/**
 * Connector which connects to remote Simulator Agent instances.
 */
@SuppressWarnings("checkstyle:classdataabstractioncoupling")
public class CoordinatorConnector implements ClientPipelineConfigurator, FailureListener {

    private static final int EXECUTOR_POOL_SIZE = Runtime.getRuntime().availableProcessors() + 1;

    private final EventLoopGroup group = new NioEventLoopGroup();
    private final AtomicLong messageIds = new AtomicLong();
    private final ClientConnectorManager clientConnectorManager = new ClientConnectorManager();
    private final ConcurrentHashMap<String, ResponseFuture> futureMap = new ConcurrentHashMap<String, ResponseFuture>();
    private final LocalExceptionLogger exceptionLogger = new LocalExceptionLogger();

    private final CoordinatorOperationProcessor processor;
    private final ExecutorService executorService;

    public CoordinatorConnector(FailureContainer failureContainer, TestPhaseListeners testPhaseListeners,
                                PerformanceStateContainer performanceStateContainer,
                                HdrHistogramContainer hdrHistogramContainer) {
        this(failureContainer, testPhaseListeners, performanceStateContainer, hdrHistogramContainer,
                createFixedThreadPool(EXECUTOR_POOL_SIZE, "CoordinatorConnector"));
    }

    CoordinatorConnector(FailureContainer failureContainer, TestPhaseListeners testPhaseListeners,
                         PerformanceStateContainer performanceStateContainer, HdrHistogramContainer hdrHistogramContainer,
                         ExecutorService executorService) {
        this.processor = new CoordinatorOperationProcessor(exceptionLogger, failureContainer, testPhaseListeners,
                performanceStateContainer, hdrHistogramContainer);
        this.executorService = executorService;
    }

    @Override
    public void configureClientPipeline(ChannelPipeline pipeline, SimulatorAddress remoteAddress,
                                        ConcurrentMap<String, ResponseFuture> futureMap) {
        pipeline.addLast("messageEncoder", new MessageEncoder(COORDINATOR, remoteAddress));
        pipeline.addLast("responseEncoder", new ResponseEncoder(COORDINATOR));
        pipeline.addLast("frameDecoder", new SimulatorFrameDecoder());
        pipeline.addLast("protocolDecoder", new SimulatorProtocolDecoder(COORDINATOR));
        pipeline.addLast("responseHandler", new ResponseHandler(COORDINATOR, remoteAddress, futureMap));
        pipeline.addLast("messageConsumeHandler", new MessageConsumeHandler(COORDINATOR, processor, executorService));
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
        for (ResponseFuture future : futureMap.values()) {
            future.unblockOnFailure(workerAddress, COORDINATOR, workerAddress.getAgentIndex());
        }
    }

    /**
     * Disconnects from all Simulator Agent instances.
     */
    public void shutdown() {
        ThreadSpawner spawner = new ThreadSpawner("shutdownClientConnectors", true);
        for (final ClientConnector agent : clientConnectorManager.getClientConnectors()) {
            spawner.spawn(new Runnable() {
                @Override
                public void run() {
                    agent.shutdown();
                }
            });
        }
        spawner.awaitCompletion();

        group.shutdownGracefully(DEFAULT_SHUTDOWN_QUIET_PERIOD, DEFAULT_SHUTDOWN_TIMEOUT, TimeUnit.SECONDS).syncUninterruptibly();

        executorService.shutdown();
        awaitTermination(executorService, 1, TimeUnit.MINUTES);
    }

    /**
     * Adds a Simulator Agent and connects to it.
     *
     * @param agentIndex the index of the Simulator Agent
     * @param agentHost  the host of the Simulator Agent
     * @param agentPort  the port of the Simulator Agent
     */
    public void addAgent(int agentIndex, String agentHost, int agentPort) {
        ClientConnector client = new ClientConnector(this, group, futureMap, COORDINATOR, COORDINATOR.getChild(agentIndex),
                agentIndex, agentHost, agentPort);
        client.start();

        clientConnectorManager.addClient(agentIndex, client);
    }

    /**
     * Removes a Simulator Agent.
     *
     * @param agentIndex the index of the remote Simulator Agent
     */
    public void removeAgent(int agentIndex) {
        clientConnectorManager.removeClient(agentIndex);
    }

    /**
     * Sends a {@link SimulatorMessage} to the addressed Simulator component.
     *
     * @param destination the {@link SimulatorAddress} of the destination
     * @param operation   the {@link SimulatorOperation} to send
     * @return a {@link Response} with the response of all addressed Simulator components.
     */
    public Response write(SimulatorAddress destination, SimulatorOperation operation) {
        SimulatorMessage message = new SimulatorMessage(destination, COORDINATOR, messageIds.incrementAndGet(),
                getOperationType(operation), toJson(operation));

        int agentAddressIndex = destination.getAgentIndex();
        Response response = new Response(message);
        List<ResponseFuture> futureList = new ArrayList<ResponseFuture>();
        if (agentAddressIndex == 0) {
            for (ClientConnector agent : clientConnectorManager.getClientConnectors()) {
                futureList.add(agent.writeAsync(message));
            }
        } else {
            ClientConnector agent = clientConnectorManager.get(agentAddressIndex);
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
        return unmodifiableCollection(clientConnectorManager.getClientConnectors());
    }

    // just for testing
    void addAgent(int agentIndex, ClientConnector agent) {
        clientConnectorManager.addClient(agentIndex, agent);
    }

    // just for testing
    ConcurrentHashMap<String, ResponseFuture> getFutureMap() {
        return futureMap;
    }
}
