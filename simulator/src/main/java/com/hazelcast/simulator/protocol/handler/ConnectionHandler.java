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

package com.hazelcast.simulator.protocol.handler;

import com.hazelcast.simulator.protocol.core.ConnectionListener;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.log4j.Logger;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.simulator.protocol.core.ResponseCodec.isResponse;
import static com.hazelcast.simulator.protocol.core.SimulatorMessageCodec.isSimulatorMessage;
import static com.hazelcast.util.EmptyStatement.ignore;
import static java.lang.String.format;

/**
 * Validates new connections by checking the magic bytes of the first incoming {@link ByteBuf}.
 *
 * Removes itself from the channel pipeline after successful validation, since big {@link ByteBuf}
 * can be split up into several chunks and just the first one contains the magic bytes.
 */
@ChannelHandler.Sharable
public class ConnectionHandler extends ChannelInboundHandlerAdapter {

    private static final int TIMEOUT_SECONDS = 10;
    private static final int MINIMUM_BYTE_BUFFER_SIZE = 8;

    private static final Logger LOGGER = Logger.getLogger(ConnectionHandler.class);

    private final CountDownLatch isConnectionValid = new CountDownLatch(1);

    private final ConnectionListener connectionListener;

    public ConnectionHandler(ConnectionListener connectionListener) {
        this.connectionListener = connectionListener;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        timeoutInvalidConnection(ctx, TIMEOUT_SECONDS);
    }

    void timeoutInvalidConnection(final ChannelHandlerContext ctx, final int timeoutSeconds) {
        final String remoteAddress = ctx.channel().remoteAddress().toString().replace("/", "");
        Thread thread = new Thread() {
            @Override
            public void run() {
                try {
                    isConnectionValid.await(timeoutSeconds, TimeUnit.SECONDS);
                    if (isConnectionValid.getCount() == 0) {
                        return;
                    }
                } catch (Exception e) {
                    ignore(e);
                }
                // the connection was not accepted in time, so we close it
                LOGGER.warn(format("No magic bytes sent for %d seconds, closing connection from %s",
                        TIMEOUT_SECONDS, ctx.channel()));
                ctx.close();
            }
        };
        thread.setName("ConnectionHandler-" + remoteAddress);
        thread.setDaemon(true);
        thread.start();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object obj) throws Exception {
        if (!(obj instanceof ByteBuf)) {
            return;
        }

        ByteBuf buf = (ByteBuf) obj;
        if (buf.readableBytes() < MINIMUM_BYTE_BUFFER_SIZE) {
            return;
        }

        if (!isSimulatorMessage(buf) && !isResponse(buf)) {
            LOGGER.warn(format("Invalid connection from %s (no magic bytes found)", ctx.channel().remoteAddress()));
            ctx.close();
            return;
        }

        // the connection is valid so we remove this handler and forward the buffer to the pipeline
        LOGGER.info(format("Valid connection from %s (magic bytes found)", ctx.channel().remoteAddress()));
        isConnectionValid.countDown();
        ctx.pipeline().remove(this);
        ctx.fireChannelRead(obj);

        ctx.channel().closeFuture().addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                connectionListener.disconnected(future.channel());
            }
        });

        connectionListener.connected(ctx.channel());
    }

    // just for testing
    void acceptConnection() {
        isConnectionValid.countDown();
    }
}
