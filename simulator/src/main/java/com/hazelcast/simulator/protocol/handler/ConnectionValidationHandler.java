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

package com.hazelcast.simulator.protocol.handler;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.log4j.Logger;

import static com.hazelcast.simulator.protocol.core.ResponseCodec.isResponse;
import static com.hazelcast.simulator.protocol.core.SimulatorMessageCodec.isSimulatorMessage;
import static java.lang.String.format;

/**
 * Validates new connections by checking the magic bytes of the first incoming {@link ByteBuf}.
 *
 * Removes itself from the channel pipeline after successful validation, since big {@link ByteBuf}
 * can be split up into several chunks and just the first one contains the magic bytes.
 */
@ChannelHandler.Sharable
public class ConnectionValidationHandler extends ChannelInboundHandlerAdapter {

    private static final int MINIMUM_BYTE_BUFFER_SIZE = 8;

    private static final Logger LOGGER = Logger.getLogger(ConnectionValidationHandler.class);

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
        ctx.pipeline().remove(this);
        ctx.fireChannelRead(obj);
    }
}
