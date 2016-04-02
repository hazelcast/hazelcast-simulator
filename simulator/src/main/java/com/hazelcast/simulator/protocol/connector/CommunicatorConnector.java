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
import com.hazelcast.simulator.protocol.operation.SimulatorOperation;
import com.hazelcast.simulator.protocol.processors.CommunicatorOperationProcessor;
import com.hazelcast.simulator.utils.ExecutorFactory;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.simulator.protocol.connector.ServerConnector.DEFAULT_SHUTDOWN_QUIET_PERIOD;
import static com.hazelcast.simulator.protocol.connector.ServerConnector.DEFAULT_SHUTDOWN_TIMEOUT;
import static com.hazelcast.simulator.protocol.core.SimulatorAddress.COORDINATOR;
import static com.hazelcast.simulator.protocol.core.SimulatorAddress.REMOTE;
import static com.hazelcast.simulator.protocol.operation.OperationCodec.toJson;
import static com.hazelcast.simulator.protocol.operation.OperationType.getOperationType;
import static com.hazelcast.simulator.utils.CommonUtils.awaitTermination;

/**
 * Connector which connects to remote Simulator Coordinator instances.
 */
@SuppressWarnings("checkstyle:classdataabstractioncoupling")
public class CommunicatorConnector implements ClientPipelineConfigurator {

    private static final int COORDINATOR_INDEX = 1;

    private final EventLoopGroup group = new NioEventLoopGroup();
    private final AtomicLong messageIds = new AtomicLong();
    private final ClientConnectorManager clientConnectorManager = new ClientConnectorManager();
    private final ConcurrentHashMap<String, ResponseFuture> futureMap = new ConcurrentHashMap<String, ResponseFuture>();
    private final ExecutorService executorService = ExecutorFactory.createFixedThreadPool(1, "CommunicatorConnector");

    private final ClientConnector client;
    private final CommunicatorOperationProcessor processor;

    public CommunicatorConnector(String coordinatorHost, int coordinatorPort) {
        LocalExceptionLogger exceptionLogger = new LocalExceptionLogger();

        client = new ClientConnector(this, group, futureMap, COORDINATOR, COORDINATOR, 1, coordinatorHost, coordinatorPort);
        processor = new CommunicatorOperationProcessor(exceptionLogger);
    }

    @Override
    public void configureClientPipeline(ChannelPipeline pipeline, SimulatorAddress remoteAddress,
                                        ConcurrentMap<String, ResponseFuture> futureMap) {
        pipeline.addLast("messageEncoder", new MessageEncoder(REMOTE, remoteAddress));
        pipeline.addLast("responseEncoder", new ResponseEncoder(REMOTE));
        pipeline.addLast("frameDecoder", new SimulatorFrameDecoder());
        pipeline.addLast("protocolDecoder", new SimulatorProtocolDecoder(REMOTE));
        pipeline.addLast("responseHandler", new ResponseHandler(REMOTE, remoteAddress, futureMap, COORDINATOR_INDEX));
        pipeline.addLast("messageConsumeHandler", new MessageConsumeHandler(REMOTE, processor, executorService));
    }

    public void start() {
        client.start();
        clientConnectorManager.addClient(COORDINATOR_INDEX, client);
    }

    /**
     * Disconnects from all Simulator Coordinator instances.
     */
    public void shutdown() {
        clientConnectorManager.removeClient(COORDINATOR_INDEX);

        group.shutdownGracefully(DEFAULT_SHUTDOWN_QUIET_PERIOD, DEFAULT_SHUTDOWN_TIMEOUT, TimeUnit.SECONDS).syncUninterruptibly();

        executorService.shutdown();
        awaitTermination(executorService, 1, TimeUnit.MINUTES);
    }

    /**
     * Sends a {@link SimulatorMessage} to the addressed Simulator component.
     *
     * @param operation the {@link SimulatorOperation} to send
     * @return a {@link Response} with the response of all addressed Simulator components.
     */
    public Response write(SimulatorOperation operation) {
        SimulatorMessage message = new SimulatorMessage(COORDINATOR, REMOTE, messageIds.incrementAndGet(),
                getOperationType(operation), toJson(operation));

        Response response = new Response(message);
        List<ResponseFuture> futureList = new ArrayList<ResponseFuture>();
        for (ClientConnector agent : clientConnectorManager.getClientConnectors()) {
            futureList.add(agent.writeAsync(message));
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

    // just for testing
    void addCoordinator(ClientConnector coordinator) {
        clientConnectorManager.addClient(COORDINATOR_INDEX, coordinator);
    }

    // just for testing
    ConcurrentHashMap<String, ResponseFuture> getFutureMap() {
        return futureMap;
    }
}
