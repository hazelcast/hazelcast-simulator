/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.simulator.protocol.handler.MessageConsumeHandler;
import com.hazelcast.simulator.protocol.handler.MessageEncoder;
import com.hazelcast.simulator.protocol.handler.ResponseEncoder;
import com.hazelcast.simulator.protocol.handler.ResponseHandler;
import com.hazelcast.simulator.protocol.handler.SimulatorFrameDecoder;
import com.hazelcast.simulator.protocol.handler.SimulatorProtocolDecoder;
import com.hazelcast.simulator.protocol.operation.SimulatorOperation;
import com.hazelcast.simulator.protocol.processors.CoordinatorRemoteOperationProcessor;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.simulator.protocol.connector.ServerConnector.DEFAULT_SHUTDOWN_QUIET_PERIOD;
import static com.hazelcast.simulator.protocol.connector.ServerConnector.DEFAULT_SHUTDOWN_TIMEOUT;
import static com.hazelcast.simulator.protocol.core.SimulatorAddress.COORDINATOR;
import static com.hazelcast.simulator.protocol.core.SimulatorAddress.REMOTE;
import static com.hazelcast.simulator.protocol.operation.OperationCodec.toJson;
import static com.hazelcast.simulator.protocol.operation.OperationType.getOperationType;
import static com.hazelcast.simulator.utils.CommonUtils.awaitTermination;
import static com.hazelcast.simulator.utils.ExecutorFactory.createFixedThreadPool;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Connector which connects to remote Simulator Coordinator instances.
 */
@SuppressWarnings("checkstyle:classdataabstractioncoupling")
public class CoordinatorRemoteConnector implements ClientPipelineConfigurator, Closeable {

    private static final int COORDINATOR_INDEX = 1;

    private final EventLoopGroup group = new NioEventLoopGroup();
    // we need to initialize the messageId's because multiple remote connectors could be connected at the same time.
    private final AtomicLong messageIds = new AtomicLong(Math.abs(new Random().nextInt(Integer.MAX_VALUE)));
    private final ClientConnectorManager clientConnectorManager = new ClientConnectorManager();
    private final ConcurrentHashMap<String, ResponseFuture> futureMap = new ConcurrentHashMap<String, ResponseFuture>();
    private final ExecutorService executorService = createFixedThreadPool(1, "CoordinatorRemoteConnector");

    private final ClientConnector client;
    private final CoordinatorRemoteOperationProcessor processor;

    public CoordinatorRemoteConnector(String coordinatorHost, int coordinatorPort) {
        this.client = new ClientConnector(this, group, futureMap, REMOTE, COORDINATOR, 1, coordinatorHost, coordinatorPort,
                true);
        this.processor = new CoordinatorRemoteOperationProcessor();
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
    @Override
    public void close() {
        clientConnectorManager.removeClient(COORDINATOR_INDEX);

        group.shutdownGracefully(DEFAULT_SHUTDOWN_QUIET_PERIOD, DEFAULT_SHUTDOWN_TIMEOUT, SECONDS).syncUninterruptibly();

        executorService.shutdown();
        awaitTermination(executorService, 1, MINUTES);
    }

    /**
     * Sends a {@link SimulatorMessage} to the addressed Simulator component.
     *
     * @param op the {@link SimulatorOperation} to send
     * @return a {@link Response} with the response of all addressed Simulator components.
     */
    public Response write(SimulatorOperation op) {
        long id = messageIds.incrementAndGet();
        SimulatorMessage message = new SimulatorMessage(COORDINATOR, REMOTE, id, getOperationType(op), toJson(op));

        Response response = new Response(message);
        List<ResponseFuture> futureList = new ArrayList<ResponseFuture>();
        for (ClientConnector agent : clientConnectorManager.getClientConnectors()) {
            futureList.add(agent.writeAsync(message));
        }
        try {
            for (ResponseFuture future : futureList) {
                response.addAllParts(future.get());
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
