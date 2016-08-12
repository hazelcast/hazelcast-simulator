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

import com.hazelcast.simulator.agent.Agent;
import com.hazelcast.simulator.agent.workerprocess.WorkerProcessManager;
import com.hazelcast.simulator.protocol.core.ConnectionManager;
import com.hazelcast.simulator.protocol.core.Response;
import com.hazelcast.simulator.protocol.core.ResponseFuture;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.handler.ConnectionListenerHandler;
import com.hazelcast.simulator.protocol.handler.ConnectionValidationHandler;
import com.hazelcast.simulator.protocol.handler.ExceptionHandler;
import com.hazelcast.simulator.protocol.handler.ForwardToCoordinatorHandler;
import com.hazelcast.simulator.protocol.handler.ForwardToWorkerHandler;
import com.hazelcast.simulator.protocol.handler.MessageConsumeHandler;
import com.hazelcast.simulator.protocol.handler.MessageEncoder;
import com.hazelcast.simulator.protocol.handler.ResponseEncoder;
import com.hazelcast.simulator.protocol.handler.ResponseHandler;
import com.hazelcast.simulator.protocol.handler.SimulatorFrameDecoder;
import com.hazelcast.simulator.protocol.handler.SimulatorProtocolDecoder;
import com.hazelcast.simulator.protocol.operation.SimulatorOperation;
import com.hazelcast.simulator.protocol.processors.AgentOperationProcessor;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.group.ChannelGroup;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.simulator.protocol.core.AddressLevel.AGENT;
import static com.hazelcast.simulator.protocol.core.SimulatorAddress.COORDINATOR;
import static java.lang.Math.max;

/**
 * Connector which listens for incoming Simulator Coordinator connections and manages Simulator Worker instances.
 */
@SuppressWarnings("checkstyle:classdataabstractioncoupling")
public class AgentConnector extends AbstractServerConnector implements ClientPipelineConfigurator {

    private final AgentOperationProcessor processor;
    private final ConcurrentMap<String, ResponseFuture> futureMap;

    private final SimulatorAddress localAddress;
    private final int addressIndex;

    private final ConnectionManager connectionManager;
    private final WorkerProcessManager workerProcessManager;

    AgentConnector(ConcurrentMap<String, ResponseFuture> futureMap, SimulatorAddress localAddress, int port, Agent agent,
                   WorkerProcessManager workerProcessManager, ConnectionManager connectionManager, int threadPoolSize) {
        super(futureMap, localAddress, port, threadPoolSize);

        this.processor = new AgentOperationProcessor(agent, workerProcessManager, getScheduledExecutor());

        this.futureMap = futureMap;

        this.localAddress = localAddress;
        this.addressIndex = localAddress.getAddressIndex();

        this.connectionManager = connectionManager;
        this.workerProcessManager = workerProcessManager;
    }

    /**
     * Creates an {@link AgentConnector} instance.
     *
     * @param agent                instance of this Simulator Agent
     * @param workerProcessManager manager for WorkerJVM instances
     * @param port                 the port for incoming connections
     * @param threadPoolSize       size of the Netty thread pool to connect to Worker instances
     * @return the {@link AgentConnector} instance
     */
    public static AgentConnector createInstance(Agent agent, WorkerProcessManager workerProcessManager,
                                                int port, int threadPoolSize) {
        ConcurrentMap<String, ResponseFuture> futureMap = new ConcurrentHashMap<String, ResponseFuture>();
        SimulatorAddress localAddress = new SimulatorAddress(AGENT, agent.getAddressIndex(), 0, 0);
        ConnectionManager connectionManager = new ConnectionManager();

        threadPoolSize = max(getDefaultThreadPoolSize(), threadPoolSize);

        return new AgentConnector(futureMap, localAddress, port, agent, workerProcessManager, connectionManager, threadPoolSize);
    }

    @Override
    public void configureClientPipeline(ChannelPipeline pipeline, SimulatorAddress remoteAddress,
                                        ConcurrentMap<String, ResponseFuture> futureMap) {
        pipeline.addLast("responseEncoder", new ResponseEncoder(localAddress));
        pipeline.addLast("messageEncoder", new MessageEncoder(localAddress, remoteAddress));
        pipeline.addLast("frameDecoder", new SimulatorFrameDecoder());
        pipeline.addLast("protocolDecoder", new SimulatorProtocolDecoder(localAddress, workerProcessManager));
        pipeline.addLast("forwardToCoordinatorHandler", new ForwardToCoordinatorHandler(localAddress, connectionManager,
                workerProcessManager));
        pipeline.addLast("responseHandler", new ResponseHandler(localAddress, remoteAddress, getFutureMap()));
        pipeline.addLast("messageConsumeHandler", new MessageConsumeHandler(localAddress, processor, getScheduledExecutor()));
        pipeline.addLast("exceptionHandler", new ExceptionHandler(this));
    }

    @Override
    void configureServerPipeline(ChannelPipeline pipeline, ServerConnector serverConnector) {
        pipeline.addLast("connectionValidationHandler", new ConnectionValidationHandler());
        pipeline.addLast("connectionListenerHandler", new ConnectionListenerHandler(connectionManager));
        pipeline.addLast("responseEncoder", new ResponseEncoder(localAddress));
        pipeline.addLast("messageEncoder", new MessageEncoder(localAddress, COORDINATOR));
        pipeline.addLast("frameDecoder", new SimulatorFrameDecoder());
        pipeline.addLast("protocolDecoder", new SimulatorProtocolDecoder(localAddress));
        pipeline.addLast("forwardToWorkerHandler", new ForwardToWorkerHandler(localAddress, getClientConnectorManager(),
                getScheduledExecutor()));
        pipeline.addLast("messageConsumeHandler", new MessageConsumeHandler(localAddress, processor, getScheduledExecutor()));
        pipeline.addLast("responseHandler", new ResponseHandler(localAddress, COORDINATOR, futureMap, addressIndex));
        pipeline.addLast("exceptionHandler", new ExceptionHandler(this));
    }

    @Override
    ChannelGroup getChannelGroup() {
        return connectionManager.getChannels();
    }

    @Override
    public Response write(SimulatorAddress destination, SimulatorOperation operation) {
        return super.write(destination, operation);
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
        SimulatorAddress remoteAddress = localAddress.getChild(workerIndex);
        ClientConnector clientConnector = new ClientConnector(this, getEventLoopGroup(), futureMap, localAddress, remoteAddress,
                workerIndex, workerHost, workerPort);
        clientConnector.start();

        getClientConnectorManager().addClient(workerIndex, clientConnector);

        return remoteAddress;
    }

    /**
     * Removes a Simulator Worker.
     *
     * @param workerIndex the index of the remote Simulator Worker
     */
    public void removeWorker(int workerIndex) {
        getClientConnectorManager().removeClient(workerIndex);
    }
}
