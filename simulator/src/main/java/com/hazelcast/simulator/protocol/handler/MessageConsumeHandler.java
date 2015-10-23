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

import com.hazelcast.simulator.protocol.core.AddressLevel;
import com.hazelcast.simulator.protocol.core.Response;
import com.hazelcast.simulator.protocol.core.ResponseType;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.core.SimulatorMessage;
import com.hazelcast.simulator.protocol.processors.OperationProcessor;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.apache.log4j.Logger;

import static com.hazelcast.simulator.protocol.operation.OperationCodec.fromSimulatorMessage;
import static java.lang.String.format;

/**
 * A {@link SimpleChannelInboundHandler} to deserialize a {@link com.hazelcast.simulator.protocol.operation.SimulatorOperation}
 * from a received {@link SimulatorMessage} and execute it on the configured {@link OperationProcessor}.
 */
public class MessageConsumeHandler extends SimpleChannelInboundHandler<SimulatorMessage> {

    private static final Logger LOGGER = Logger.getLogger(MessageConsumeHandler.class);

    private final SimulatorAddress localAddress;
    private final AddressLevel addressLevel;

    private final OperationProcessor processor;

    public MessageConsumeHandler(SimulatorAddress localAddress, OperationProcessor processor) {
        this.localAddress = localAddress;
        this.addressLevel = localAddress.getAddressLevel();

        this.processor = processor;
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, SimulatorMessage msg) {
        long messageId = msg.getMessageId();
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("[%d] %s %s MessageConsumeHandler is consuming message...", messageId, addressLevel,
                    localAddress));
        }

        ResponseType responseType = processor.process(fromSimulatorMessage(msg), msg.getSource());
        ctx.writeAndFlush(new Response(messageId, msg.getSource(), localAddress, responseType));
    }
}
