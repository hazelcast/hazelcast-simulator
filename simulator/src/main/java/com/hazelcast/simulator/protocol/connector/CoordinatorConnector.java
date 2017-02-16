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

import com.hazelcast.simulator.protocol.core.ConnectionManager;
import com.hazelcast.simulator.protocol.core.Response;
import com.hazelcast.simulator.protocol.core.ResponseFuture;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.handler.ConnectionHandler;
import com.hazelcast.simulator.protocol.handler.ExceptionHandler;
import com.hazelcast.simulator.protocol.handler.MessageConsumeHandler;
import com.hazelcast.simulator.protocol.handler.MessageEncoder;
import com.hazelcast.simulator.protocol.handler.ResponseEncoder;
import com.hazelcast.simulator.protocol.handler.ResponseHandler;
import com.hazelcast.simulator.protocol.handler.SimulatorFrameDecoder;
import com.hazelcast.simulator.protocol.handler.SimulatorProtocolDecoder;
import com.hazelcast.simulator.protocol.operation.SimulatorOperation;
import com.hazelcast.simulator.protocol.processors.CoordinatorOperationProcessor;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.group.ChannelGroup;

import java.util.Collection;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.simulator.protocol.core.SimulatorAddress.COORDINATOR;
import static com.hazelcast.simulator.protocol.core.SimulatorAddress.REMOTE;
import static java.util.Collections.unmodifiableCollection;

/**
 * Connector for the Simulator Coordinator which connects to remote Simulator Agent instances.
 */
public class CoordinatorConnector extends AbstractServerConnector implements ClientPipelineConfigurator {

    private final CoordinatorOperationProcessor processor;
    private final ConnectionManager connectionManager = new ConnectionManager();

    public CoordinatorConnector(CoordinatorOperationProcessor processor, int port) {
        super(COORDINATOR, port, getDefaultThreadPoolSize());
        this.processor = processor;
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
        pipeline.addLast("connectionListenerHandler", new ConnectionHandler(connectionManager));
        pipeline.addLast("responseEncoder", new ResponseEncoder(COORDINATOR));
        pipeline.addLast("messageEncoder", new MessageEncoder(COORDINATOR, COORDINATOR));
        pipeline.addLast("frameDecoder", new SimulatorFrameDecoder());
        pipeline.addLast("protocolDecoder", new SimulatorProtocolDecoder(COORDINATOR));
        pipeline.addLast("messageConsumeHandler", new MessageConsumeHandler(COORDINATOR, processor, getScheduledExecutor()));
        pipeline.addLast("responseHandler", new ResponseHandler(COORDINATOR, REMOTE, getFutureMap(), 0));
        pipeline.addLast("exceptionHandler", new ExceptionHandler(this));
    }

    @Override
    ChannelGroup getChannelGroup() {
        return connectionManager.getChannels();
    }

    @Override
    public Response invoke(SimulatorAddress destination, SimulatorOperation op) {
        return super.invoke(destination, op);
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
                COORDINATOR.getChild(agentIndex), agentIndex, agentHost, agentPort, true);
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

    public Response writeToRemoteController(SimulatorOperation op) {
        return super.invoke(REMOTE, op);
    }

    // just for testing
    public Collection<ClientConnector> getClientConnectors() {
        return unmodifiableCollection(getClientConnectorManager().getClientConnectors());
    }
}
