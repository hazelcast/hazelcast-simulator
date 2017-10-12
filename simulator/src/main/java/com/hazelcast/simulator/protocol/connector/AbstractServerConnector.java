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
import com.hazelcast.simulator.protocol.core.ResponseType;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.core.SimulatorMessage;
import com.hazelcast.simulator.protocol.core.SimulatorProtocolException;
import com.hazelcast.simulator.protocol.operation.OperationTypeCounter;
import com.hazelcast.simulator.protocol.operation.SimulatorOperation;
import com.hazelcast.simulator.utils.ThreadSpawner;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.apache.log4j.Logger;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.simulator.protocol.core.ResponseFuture.createFutureKey;
import static com.hazelcast.simulator.protocol.core.ResponseFuture.createInstance;
import static com.hazelcast.simulator.protocol.core.ResponseType.EXCEPTION_DURING_OPERATION_EXECUTION;
import static com.hazelcast.simulator.protocol.core.ResponseType.FAILURE_AGENT_NOT_FOUND;
import static com.hazelcast.simulator.protocol.core.ResponseType.SUCCESS;
import static com.hazelcast.simulator.protocol.core.SimulatorAddress.COORDINATOR;
import static com.hazelcast.simulator.protocol.operation.OperationCodec.toJson;
import static com.hazelcast.simulator.protocol.operation.OperationType.getOperationType;
import static com.hazelcast.simulator.utils.CommonUtils.awaitTermination;
import static com.hazelcast.simulator.utils.CommonUtils.joinThread;
import static com.hazelcast.simulator.utils.CommonUtils.sleepMillis;
import static com.hazelcast.simulator.utils.ExecutorFactory.createScheduledThreadPool;
import static java.lang.Math.max;
import static java.lang.Runtime.getRuntime;
import static java.lang.String.format;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Abstract {@link ServerConnector} class for Simulator Agent and Worker.
 */
abstract class AbstractServerConnector implements ServerConnector {

    private static final int MIN_THREAD_POOL_SIZE = 10;
    private static final int DEFAULT_THREAD_POOL_SIZE = max(MIN_THREAD_POOL_SIZE, getRuntime().availableProcessors() * 2);

    private static final Logger LOGGER = Logger.getLogger(AbstractServerConnector.class);
    private static final SimulatorMessage POISON_PILL = new SimulatorMessage(null, null, 0, null, null);

    protected final ConcurrentMap<String, ResponseFuture> futureMap = new ConcurrentHashMap<String, ResponseFuture>();
    protected final SimulatorAddress localAddress;

    private final String className = getClass().getSimpleName();
    private final AtomicBoolean isStarted = new AtomicBoolean();
    private final ClientConnectorManager clientConnectorManager = new ClientConnectorManager();

    private final AtomicLong messageIds = new AtomicLong();
    private final ConcurrentMap<String, ResponseFuture> messageQueueFutures = new ConcurrentHashMap<String, ResponseFuture>();
    private final BlockingQueue<SimulatorMessage> messageQueue = new LinkedBlockingQueue<SimulatorMessage>();
    private final MessageQueueThread messageQueueThread = new MessageQueueThread();

    private final int addressIndex;
    private final int port;

    private final EventLoopGroup group;
    private final ScheduledExecutorService executorService;

    private Channel channel;

    AbstractServerConnector(SimulatorAddress localAddress, int port, int threadPoolSize) {
        this(localAddress, port, threadPoolSize, createScheduledThreadPool(threadPoolSize, "AbstractServerConnector"));
    }

    AbstractServerConnector(SimulatorAddress localAddress, int port,
                            int threadPoolSize, ScheduledExecutorService executorService) {
        this.localAddress = localAddress;
        this.addressIndex = COORDINATOR.equals(localAddress) ? 0 : localAddress.getAddressIndex();
        this.port = port;
        this.group = new NioEventLoopGroup(threadPoolSize);
        this.executorService = executorService;
    }

    abstract void configureServerPipeline(ChannelPipeline pipeline, ServerConnector serverConnector);

    abstract ChannelGroup getChannelGroup();

    @Override
    public void start() {
        if (!isStarted.compareAndSet(false, true)) {
            throw new SimulatorProtocolException(format("%s cannot be started twice or after shutdown!", className));
        }

        messageQueueThread.start();
        if (port > 0) {
            ServerBootstrap bootstrap = getServerBootstrap();
            ChannelFuture future = bootstrap.bind().syncUninterruptibly();
            channel = future.channel();

            LOGGER.info(format("%s %s listens on %s", className, localAddress, channel.localAddress()));
        }
    }

    private ServerBootstrap getServerBootstrap() {
        return new ServerBootstrap()
                .group(group)
                .channel(NioServerSocketChannel.class)
                .localAddress(new InetSocketAddress(port))
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel channel) {
                        channel.config().setReuseAddress(true);
                        configureServerPipeline(channel.pipeline(), AbstractServerConnector.this);
                    }
                });
    }

    @Override
    public void close() {
        LOGGER.info(format("Shutdown of %s...", className));
        if (!isStarted.compareAndSet(true, false)) {
            throw new SimulatorProtocolException(format("%s cannot be shutdown twice or if not been started!", className));
        }

        ThreadSpawner spawner = new ThreadSpawner("shutdownClientConnectors", true);
        for (final ClientConnector client : clientConnectorManager.getClientConnectors()) {
            spawner.spawn(new Runnable() {
                @Override
                public void run() {
                    client.shutdown();
                }
            });
        }
        spawner.awaitCompletion();

        messageQueueThread.shutdown();
        if (channel != null) {
            channel.close().syncUninterruptibly();
        }
        group.shutdownGracefully(DEFAULT_SHUTDOWN_QUIET_PERIOD, DEFAULT_SHUTDOWN_TIMEOUT, SECONDS).syncUninterruptibly();

        executorService.shutdown();
        awaitTermination(executorService, 1, TimeUnit.MINUTES);
    }

    @Override
    public SimulatorAddress getAddress() {
        return localAddress;
    }

    @Override
    public int getPort() {
        return port;
    }

    @Override
    public ConcurrentMap<String, ResponseFuture> getFutureMap() {
        return futureMap;
    }

    @Override
    public ResponseFuture submit(SimulatorAddress destination, SimulatorOperation op) {
        checkNoWildcardAllowed(destination);

        return submit(localAddress, destination, op);
    }

    @Override
    public Response invoke(SimulatorAddress destination, SimulatorOperation op) {
        return invoke(localAddress, destination, op);
    }

    @Override
    public Response invoke(SimulatorAddress source, SimulatorAddress destination, SimulatorOperation op) {
        SimulatorMessage message = createSimulatorMessage(source, destination, op);
        Response response = new Response(message);

        List<ResponseFuture> futureList = invokeAsync(message);
        try {
            for (ResponseFuture future : futureList) {
                response.addAllParts(future.get());
            }
        } catch (InterruptedException e) {
            throw new SimulatorProtocolException("ResponseFuture.get() got interrupted!", e);
        }
        return response;
    }

    @Override
    public ResponseFuture invokeAsync(SimulatorAddress destination, SimulatorOperation op) {
        return invokeAsync(localAddress, destination, op);
    }

    @Override
    public ResponseFuture invokeAsync(SimulatorAddress source, SimulatorAddress destination, SimulatorOperation op) {
        checkNoWildcardAllowed(destination);

        SimulatorMessage message = createSimulatorMessage(source, destination, op);
        return invokeAsync(message).get(0);
    }

    static int getDefaultThreadPoolSize() {
        return DEFAULT_THREAD_POOL_SIZE;
    }

    ClientConnectorManager getClientConnectorManager() {
        return clientConnectorManager;
    }

    EventLoopGroup getEventLoopGroup() {
        return group;
    }

    ScheduledExecutorService getScheduledExecutor() {
        return executorService;
    }

    int getMessageQueueSizeInternal() {
        return messageQueue.size();
    }

    @Override
    public ResponseFuture submit(SimulatorAddress source, SimulatorAddress destination, SimulatorOperation op) {
        SimulatorMessage message = createSimulatorMessage(source, destination, op);
        String futureKey = createFutureKey(source, message.getMessageId(), 0);
        ResponseFuture responseFuture = createInstance(messageQueueFutures, futureKey);
        messageQueue.add(message);
        return responseFuture;
    }

    private SimulatorMessage createSimulatorMessage(SimulatorAddress src, SimulatorAddress dst, SimulatorOperation op) {
        return new SimulatorMessage(dst, src, messageIds.incrementAndGet(), getOperationType(op), toJson(op));
    }

    private List<ResponseFuture> invokeAsync(SimulatorMessage message) {
        if (localAddress.getAddressLevel().isParentAddressLevel(message.getDestination().getAddressLevel())) {
            // we have to send the message to the connected children
            return writeAsyncToChildren(message, message.getDestination().getAgentIndex());
        } else {
            // we have to send the message to the connected parents
            return singletonList(writeAsyncToParents(message));
        }
    }

    private List<ResponseFuture> writeAsyncToChildren(SimulatorMessage message, int agentAddressIndex) {
        List<ResponseFuture> futureList = new ArrayList<ResponseFuture>();
        if (agentAddressIndex == 0) {
            for (ClientConnector agent : getClientConnectorManager().getClientConnectors()) {
                futureList.add(agent.writeAsync(message));
            }
        } else {
            ClientConnector agent = getClientConnectorManager().get(agentAddressIndex);
            if (agent == null) {
                futureList.add(createResponseFuture(message, FAILURE_AGENT_NOT_FOUND));
            } else {
                futureList.add(agent.writeAsync(message));
            }
        }
        return futureList;
    }

    private ResponseFuture writeAsyncToParents(SimulatorMessage message) {
        long messageId = message.getMessageId();
        String futureKey = createFutureKey(message.getSource(), messageId, addressIndex);
        ResponseFuture future = createInstance(futureMap, futureKey);
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("[%d] %s created ResponseFuture %s", messageId, localAddress, futureKey));
        }
        OperationTypeCounter.sent(message.getOperationType());
        getChannelGroup().writeAndFlush(message);

        return future;
    }

    private ResponseFuture createResponseFuture(SimulatorMessage message, ResponseType responseType) {
        long messageId = message.getMessageId();
        SimulatorAddress destination = message.getDestination();
        String futureKey = createFutureKey(message.getSource(), messageId, destination.getAddressIndex());

        ResponseFuture future = createInstance(futureMap, futureKey);
        future.set(new Response(messageId, destination, message.getSource(), responseType));
        return future;
    }

    private void checkNoWildcardAllowed(SimulatorAddress destination) {
        if (destination.containsWildcard()) {
            throw new IllegalArgumentException("This method is not allowed for a wildcard destination!");
        }
    }

    private final class MessageQueueThread extends Thread {

        private static final int WAIT_FOR_EMPTY_QUEUE_MILLIS = 100;

        private MessageQueueThread() {
            super("MessageQueueThread");
        }

        @Override
        public void run() {
            while (true) {
                SimulatorMessage message = null;
                ResponseFuture responseFuture = null;
                Response response = null;
                try {
                    message = messageQueue.take();
                    if (POISON_PILL.equals(message)) {
                        LOGGER.info("MessageQueueThread received POISON_PILL and will stop...");
                        break;
                    }

                    String futureKey = createFutureKey(message.getSource(), message.getMessageId(), 0);
                    responseFuture = messageQueueFutures.get(futureKey);

                    response = invokeAsync(message).get(0).get();
                } catch (Exception e) {
                    LOGGER.error("Error while sending message from messageQueue", e);

                    if (message != null) {
                        response = new Response(message, EXCEPTION_DURING_OPERATION_EXECUTION);
                    }
                }
                if (response != null) {
                    if (responseFuture != null) {
                        responseFuture.set(response);
                    }

                    ResponseType responseType = response.getFirstErrorResponseType();
                    if (!responseType.equals(SUCCESS)) {
                        LOGGER.error("Got response type " + responseType + " for " + message);
                    }
                }
            }
        }

        public void shutdown() {
            messageQueue.add(POISON_PILL);

            SimulatorMessage message = messageQueue.peek();
            while (message != null) {
                if (!POISON_PILL.equals(message)) {
                    int queueSize = messageQueue.size();
                    LOGGER.debug(format("%d messages pending on messageQueue, first message: %s", queueSize, message));
                }
                sleepMillis(WAIT_FOR_EMPTY_QUEUE_MILLIS);
                message = messageQueue.peek();
            }

            joinThread(messageQueueThread);
        }
    }
}
