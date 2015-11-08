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

import com.hazelcast.simulator.protocol.core.Response;
import com.hazelcast.simulator.protocol.core.ResponseFuture;
import com.hazelcast.simulator.protocol.core.ResponseType;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.core.SimulatorMessage;
import com.hazelcast.simulator.protocol.core.SimulatorProtocolException;
import com.hazelcast.simulator.protocol.operation.SimulatorOperation;
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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.simulator.protocol.core.ResponseFuture.createFutureKey;
import static com.hazelcast.simulator.protocol.core.ResponseFuture.createInstance;
import static com.hazelcast.simulator.protocol.operation.OperationCodec.toJson;
import static com.hazelcast.simulator.protocol.operation.OperationType.getOperationType;
import static com.hazelcast.simulator.utils.CommonUtils.joinThread;
import static com.hazelcast.simulator.utils.CommonUtils.sleepMillis;
import static java.lang.String.format;

/**
 * Abstract {@link ServerConnector} class for Simulator Agent and Worker.
 */
abstract class AbstractServerConnector implements ServerConnector {

    private static final Logger LOGGER = Logger.getLogger(AbstractServerConnector.class);

    private static final SimulatorMessage POISON_PILL = new SimulatorMessage(null, null, 0, null, null);

    private final EventLoopGroup bossGroup = new NioEventLoopGroup();
    private final EventLoopGroup workerGroup = new NioEventLoopGroup();

    private final AtomicLong messageIds = new AtomicLong();
    private final ConcurrentMap<String, ResponseFuture> messageQueueFutures = new ConcurrentHashMap<String, ResponseFuture>();
    private final BlockingQueue<SimulatorMessage> messageQueue = new LinkedBlockingQueue<SimulatorMessage>();
    private final MessageQueueThread messageQueueThread = new MessageQueueThread();

    private final ConcurrentMap<String, ResponseFuture> futureMap;
    private final SimulatorAddress localAddress;
    private final int addressIndex;
    private final int port;

    private Channel channel;

    AbstractServerConnector(ConcurrentMap<String, ResponseFuture> futureMap, SimulatorAddress localAddress, int port) {
        this.futureMap = futureMap;
        this.localAddress = localAddress;
        this.addressIndex = localAddress.getAddressIndex();
        this.port = port;
    }

    abstract void configureServerPipeline(ChannelPipeline pipeline, ServerConnector serverConnector);

    abstract void connectorShutdown();

    abstract ChannelGroup getChannelGroup();

    @Override
    public void start() {
        messageQueueThread.start();

        ServerBootstrap bootstrap = getServerBootstrap();
        ChannelFuture future = bootstrap.bind().syncUninterruptibly();
        channel = future.channel();

        LOGGER.info(format("ServerConnector %s listens on %s", localAddress, channel.localAddress()));
    }

    private ServerBootstrap getServerBootstrap() {
        ServerBootstrap bootstrap = new ServerBootstrap();
        bootstrap.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .localAddress(new InetSocketAddress(port))
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel channel) {
                        configureServerPipeline(channel.pipeline(), AbstractServerConnector.this);
                    }
                });
        return bootstrap;
    }

    @Override
    public void shutdown() {
        messageQueueThread.shutdown();
        connectorShutdown();
        channel.close().syncUninterruptibly();

        workerGroup.
                shutdownGracefully(DEFAULT_SHUTDOWN_QUIET_PERIOD, DEFAULT_SHUTDOWN_TIMEOUT, TimeUnit.SECONDS)
                .syncUninterruptibly();
        bossGroup
                .shutdownGracefully(DEFAULT_SHUTDOWN_QUIET_PERIOD, DEFAULT_SHUTDOWN_TIMEOUT, TimeUnit.SECONDS)
                .syncUninterruptibly();
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
    public ResponseFuture submit(SimulatorAddress destination, SimulatorOperation operation) {
        return submit(localAddress, destination, operation);
    }

    @Override
    public Response write(SimulatorAddress destination, SimulatorOperation operation) {
        return write(localAddress, destination, operation);
    }

    @Override
    public Response write(SimulatorAddress source, SimulatorAddress destination, SimulatorOperation operation) {
        try {
            return writeAsync(source, destination, operation).get();
        } catch (InterruptedException e) {
            throw new SimulatorProtocolException("ResponseFuture.get() got interrupted!", e);
        }
    }

    @Override
    public ResponseFuture writeAsync(SimulatorAddress destination, SimulatorOperation operation) {
        return writeAsync(localAddress, destination, operation);
    }

    @Override
    public ResponseFuture writeAsync(SimulatorAddress source, SimulatorAddress destination, SimulatorOperation operation) {
        SimulatorMessage message = createSimulatorMessage(source, destination, operation);
        return writeAsync(message);
    }

    ResponseFuture submit(SimulatorAddress source, SimulatorAddress destination, SimulatorOperation operation) {
        SimulatorMessage message = createSimulatorMessage(source, destination, operation);
        String futureKey = createFutureKey(source, message.getMessageId(), 0);
        ResponseFuture responseFuture = createInstance(messageQueueFutures, futureKey);
        messageQueue.add(message);
        return responseFuture;
    }

    private SimulatorMessage createSimulatorMessage(SimulatorAddress src, SimulatorAddress dst, SimulatorOperation op) {
        return new SimulatorMessage(dst, src, messageIds.incrementAndGet(), getOperationType(op), toJson(op));
    }

    private ResponseFuture writeAsync(SimulatorMessage message) {
        long messageId = message.getMessageId();
        String futureKey = createFutureKey(message.getSource(), messageId, addressIndex);
        ResponseFuture future = createInstance(futureMap, futureKey);
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("[%d] %s created ResponseFuture %s", messageId, localAddress, futureKey));
        }
        getChannelGroup().writeAndFlush(message);

        return future;
    }

    private final class MessageQueueThread extends Thread {

        private static final int WAIT_FOR_EMPTY_QUEUE_MILLIS = 100;

        private MessageQueueThread() {
            super("ServerConnectorMessageQueueThread");
        }

        @Override
        public void run() {
            while (true) {
                try {
                    SimulatorMessage message = messageQueue.take();
                    if (POISON_PILL.equals(message)) {
                        break;
                    }

                    Response response = writeAsync(message).get();

                    String futureKey = createFutureKey(message.getSource(), message.getMessageId(), 0);
                    ResponseFuture responseFuture = messageQueueFutures.get(futureKey);
                    if (responseFuture != null) {
                        responseFuture.set(response);
                    }

                    ResponseType responseType = response.getFirstErrorResponseType();
                    if (!responseType.equals(ResponseType.SUCCESS)) {
                        LOGGER.error("Got response type " + responseType + " for " + message);
                    }
                } catch (Exception e) {
                    LOGGER.error("Error while sending message from messageQueue", e);
                    throw new SimulatorProtocolException("Error while sending message from messageQueue", e);
                }
            }
        }

        public void shutdown() {
            messageQueue.add(POISON_PILL);

            SimulatorMessage message = messageQueue.peek();
            while (message != null) {
                if (!POISON_PILL.equals(message)) {
                    int queueSize = messageQueue.size();
                    LOGGER.info(format("%d messages pending on messageQueue, first message: %s", queueSize, message));
                }
                sleepMillis(WAIT_FOR_EMPTY_QUEUE_MILLIS);
                message = messageQueue.peek();
            }

            joinThread(messageQueueThread);
        }
    }
}
