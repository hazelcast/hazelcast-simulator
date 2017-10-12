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

import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.core.SimulatorMessage;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import org.apache.log4j.Logger;

import static com.hazelcast.simulator.protocol.core.SimulatorMessageCodec.encodeByteBuf;
import static java.lang.String.format;

/**
 * A {@link MessageToByteEncoder} to encode a {@link SimulatorMessage} to a {@link ByteBuf}.
 */
public class MessageEncoder extends MessageToByteEncoder<SimulatorMessage> {

    private static final Logger LOGGER = Logger.getLogger(MessageEncoder.class);

    private final SimulatorAddress localAddress;
    private final SimulatorAddress targetAddress;

    public MessageEncoder(SimulatorAddress localAddress, SimulatorAddress targetAddress) {
        this.localAddress = localAddress;
        this.targetAddress = targetAddress;
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, SimulatorMessage msg, ByteBuf out) throws Exception {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("[%d] MessageEncoder.encode() %s -> %s %s", msg.getMessageId(), localAddress, targetAddress,
                    msg));
        }
        encodeByteBuf(msg, out);
    }
}
