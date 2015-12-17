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
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.log4j.Logger;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.simulator.protocol.core.ResponseFuture.createFutureKey;
import static com.hazelcast.simulator.protocol.core.ResponseFuture.createInstance;
import static com.hazelcast.simulator.protocol.core.ResponseFuture.getMessageIdFromFutureKey;
import static com.hazelcast.simulator.protocol.core.ResponseFuture.getSourceFromFutureKey;
import static com.hazelcast.simulator.protocol.core.SimulatorMessageCodec.getMessageId;
import static com.hazelcast.simulator.protocol.core.SimulatorMessageCodec.getSourceAddress;
import static java.lang.String.format;

/**
 * Client connector for a Simulator Coordinator or Agent.
 */
public class ClientConnector {

    private static final int CONNECT_TIMEOUT_MILLIS = (int) TimeUnit.MINUTES.toMillis(2);

    private static final Logger LOGGER = Logger.getLogger(ClientConnector.class);

    private final ClientPipelineConfigurator pipelineConfigurator;
    private final EventLoopGroup group;
    private final ConcurrentMap<String, ResponseFuture> futureMap;

    private final SimulatorAddress localAddress;
    private final SimulatorAddress remoteAddress;

    private final int remoteIndex;
    private final String remoteHost;
    private final int remotePort;

    private Channel channel;

    public ClientConnector(ClientPipelineConfigurator pipelineConfigurator, EventLoopGroup group,
                           ConcurrentMap<String, ResponseFuture> futureMap, SimulatorAddress localAddress,
                           SimulatorAddress remoteAddress, int remoteIndex, String remoteHost, int remotePort) {
        this.pipelineConfigurator = pipelineConfigurator;
        this.group = group;
        this.futureMap = futureMap;

        this.localAddress = localAddress;
        this.remoteAddress = remoteAddress;

        this.remoteIndex = remoteIndex;
        this.remoteHost = remoteHost;
        this.remotePort = remotePort;
    }

    public void start() {
        Bootstrap bootstrap = getBootstrap();
        ChannelFuture future = bootstrap.connect().syncUninterruptibly();
        channel = future.channel();

        LOGGER.info(format("ClientConnector %s -> %s sends to %s", localAddress, remoteAddress, channel.remoteAddress()));
    }

    private Bootstrap getBootstrap() {
        Bootstrap bootstrap = new Bootstrap();
        bootstrap
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
        return bootstrap;
    }

    public void shutdown() {
        if (channel.isOpen()) {
            channel.close().syncUninterruptibly();
        }

        // take care about eventually pending ResponseFuture instances
        handlePendingResponseFutures();
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

    public Response write(SimulatorMessage message) {
        ResponseFuture future = writeAsync(message);
        return getResponse(future);
    }

    public Response write(ByteBuf buffer) {
        ResponseFuture future = writeAsync(buffer);
        return getResponse(future);
    }

    public ResponseFuture writeAsync(SimulatorMessage message) {
        return writeAsync(message.getSource(), message.getMessageId(), message);
    }

    public ResponseFuture writeAsync(ByteBuf buffer) {
        return writeAsync(getSourceAddress(buffer), getMessageId(buffer), buffer);
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

    private Response getResponse(ResponseFuture future) {
        try {
            return future.get();
        } catch (InterruptedException e) {
            throw new SimulatorProtocolException("ResponseFuture.get() got interrupted!", e);
        }
    }

    private void handlePendingResponseFutures() {
        for (Map.Entry<String, ResponseFuture> futureEntry : futureMap.entrySet()) {
            String futureKey = futureEntry.getKey();
            LOGGER.warn(format("ResponseFuture %s still pending after shutdown!", futureKey));
            Response response = new Response(getMessageIdFromFutureKey(futureKey), getSourceFromFutureKey(futureKey));
            response.addResponse(localAddress, ResponseType.EXCEPTION_DURING_OPERATION_EXECUTION);
            futureEntry.getValue().set(response);
        }
    }
}
