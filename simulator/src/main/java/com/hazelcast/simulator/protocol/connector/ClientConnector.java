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

import com.hazelcast.simulator.protocol.core.ResponseFuture;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.core.SimulatorMessage;
import com.hazelcast.simulator.protocol.operation.OperationTypeCounter;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.log4j.Logger;

import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.simulator.protocol.core.ResponseFuture.createFutureKey;
import static com.hazelcast.simulator.protocol.core.ResponseFuture.createInstance;
import static com.hazelcast.simulator.protocol.core.SimulatorMessageCodec.getMessageId;
import static com.hazelcast.simulator.protocol.core.SimulatorMessageCodec.getSourceAddress;
import static com.hazelcast.simulator.protocol.operation.OperationType.AUTH;
import static com.hazelcast.simulator.utils.CommonUtils.rethrow;
import static com.hazelcast.simulator.utils.CommonUtils.sleepMillis;
import static java.lang.String.format;

/**
 * Client connector for a Simulator Coordinator or Agent.
 */
public class ClientConnector {

    private static final int CONNECT_TIMEOUT_MILLIS = (int) TimeUnit.MINUTES.toMillis(2);
    private static final int CONNECT_RETRY_DELAY_MILLIS = 500;
    private static final int CONNECT_RETRIES = 5;

    private static final Logger LOGGER = Logger.getLogger(ClientConnector.class);

    private final AtomicBoolean shutdownInvoked = new AtomicBoolean();

    private final ClientPipelineConfigurator pipelineConfigurator;
    private final EventLoopGroup group;
    private final ConcurrentMap<String, ResponseFuture> futureMap;

    private final SimulatorAddress localAddress;
    private final SimulatorAddress remoteAddress;

    private final int remoteIndex;
    private final String remoteHost;
    private final int remotePort;
    private final boolean reconnectOnClose;

    private Channel channel;

    ClientConnector(ClientPipelineConfigurator pipelineConfigurator,
                    EventLoopGroup group,
                    ConcurrentMap<String, ResponseFuture> futureMap,
                    SimulatorAddress localAddress,
                    SimulatorAddress remoteAddress,
                    int remoteIndex,
                    String remoteHost,
                    int remotePort, boolean reconnectOnClose) {
        this.pipelineConfigurator = pipelineConfigurator;
        this.group = group;
        this.futureMap = futureMap;

        this.localAddress = localAddress;
        this.remoteAddress = remoteAddress;

        this.remoteIndex = remoteIndex;
        this.remoteHost = remoteHost;
        this.remotePort = remotePort;
        this.reconnectOnClose = reconnectOnClose;
    }

    public void start() {
        Bootstrap bootstrap = getBootstrap();
        connect(bootstrap, CONNECT_RETRY_DELAY_MILLIS, CONNECT_RETRIES);
    }

    private Bootstrap getBootstrap() {
        return new Bootstrap()
                .group(group)
                .channel(NioSocketChannel.class)
                .remoteAddress(new InetSocketAddress(remoteHost, remotePort))
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, CONNECT_TIMEOUT_MILLIS)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel channel) {
                        pipelineConfigurator.configureClientPipeline(channel.pipeline(), remoteAddress, futureMap);
                    }
                });
    }

    void connect(final Bootstrap bootstrap, long connectRetryDelayMillis, int connectRetries) {
        Exception exception = null;
        int connectionTry = 1;
        do {
            try {
                ChannelFuture future = bootstrap.connect().syncUninterruptibly();
                if (future.isSuccess()) {
                    channel = future.channel();
                    if (LOGGER.isTraceEnabled()) {
                        LOGGER.trace(format("ClientConnector %s -> %s sends to %s", localAddress, remoteAddress,
                                channel.remoteAddress()));
                    }
                    channel.closeFuture().addListener(new ChannelFutureListener() {
                        @Override
                        public void operationComplete(ChannelFuture future) throws Exception {
                            if (reconnectOnClose && !shutdownInvoked.get()) {
                                LOGGER.error(format("Connection %s -> %s (%s) closed! Reconnecting...", localAddress,
                                        remoteAddress, future.channel().remoteAddress()));
                                connect(bootstrap, CONNECT_RETRY_DELAY_MILLIS, CONNECT_RETRIES);
                            }
                        }
                    });

                    sendAuthMessage();
                    return;
                }
                future.channel().close();
            } catch (Exception e) {
                exception = e;
                LOGGER.warn(format("Connection refused, retrying to connect %s -> %s (%d / %d)...", localAddress, remoteAddress,
                        connectionTry, connectRetries));
                sleepMillis(connectRetryDelayMillis * connectionTry);
            }
        } while (connectionTry++ < connectRetries);

        throw rethrow(exception);
    }

    private void sendAuthMessage() {
        SimulatorMessage message = new SimulatorMessage(remoteAddress, localAddress, 0, AUTH, "AUTH");
        writeAsync(message);
    }

    public void shutdown() {
        shutdownInvoked.set(true);
        if (channel.isOpen()) {
            channel.close().syncUninterruptibly();
        }
    }

    public ConcurrentMap<String, ResponseFuture> getFutureMap() {
        return futureMap;
    }

    public SimulatorAddress getRemoteAddress() {
        return remoteAddress;
    }

    public void forwardToChannel(ByteBuf buffer) {
        channel.writeAndFlush(buffer);
    }

    public ResponseFuture writeAsync(ByteBuf buffer) {
        return writeAsync(getSourceAddress(buffer), getMessageId(buffer), buffer);
    }

    ResponseFuture writeAsync(SimulatorMessage message) {
        OperationTypeCounter.sent(message.getOperationType());
        return writeAsync(message.getSource(), message.getMessageId(), message);
    }

    private ResponseFuture writeAsync(SimulatorAddress source, long messageId, Object msg) {
        String futureKey = createFutureKey(source, messageId, remoteIndex);
        ResponseFuture future = createInstance(futureMap, futureKey);
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("[%d] %s created ResponseFuture %s", messageId, localAddress, futureKey));
        }
        channel.writeAndFlush(msg);

        return future;
    }
}
