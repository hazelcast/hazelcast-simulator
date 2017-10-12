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

import com.hazelcast.simulator.protocol.core.Response;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.core.SimulatorMessage;
import com.hazelcast.simulator.protocol.core.TestProcessorManager;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.AttributeKey;
import org.apache.log4j.Logger;

import java.util.concurrent.ExecutorService;

import static com.hazelcast.simulator.protocol.operation.OperationCodec.fromSimulatorMessage;
import static java.lang.String.format;

/**
 * A {@link SimpleChannelInboundHandler} to to deserialize a {@link com.hazelcast.simulator.protocol.operation.SimulatorOperation}
 * from a received {@link SimulatorMessage} and execute it on the {@link TestProcessorManager} of the addressed Simulator Test.
 */
public class MessageTestConsumeHandler extends SimpleChannelInboundHandler<SimulatorMessage> {

    private static final Logger LOGGER = Logger.getLogger(MessageTestConsumeHandler.class);

    private final AttributeKey<Integer> forwardAddressIndex = AttributeKey.valueOf("forwardAddressIndex");

    private final TestProcessorManager testProcessorManager;
    private final SimulatorAddress localAddress;
    private final ExecutorService executorService;

    public MessageTestConsumeHandler(TestProcessorManager testProcessorManager, SimulatorAddress localAddress,
                                     ExecutorService executorService) {
        this.testProcessorManager = testProcessorManager;
        this.localAddress = localAddress;
        this.executorService = executorService;
    }

    @Override
    public void channelRead0(final ChannelHandlerContext ctx, final SimulatorMessage msg) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("[%d] %s MessageTestConsumeHandler is consuming message...", msg.getMessageId(), localAddress));
        }

        final int testAddressIndex = ctx.attr(forwardAddressIndex).get();
        executorService.submit(new Runnable() {
            @Override
            public void run() {
                Response response = new Response(msg);
                if (testAddressIndex == 0) {
                    if (LOGGER.isTraceEnabled()) {
                        LOGGER.trace(format("[%d] forwarding message to all tests", msg.getMessageId()));
                    }
                    testProcessorManager.processOnAllTests(response, fromSimulatorMessage(msg), msg.getSource());
                } else {
                    if (LOGGER.isTraceEnabled()) {
                        LOGGER.trace(format("[%d] forwarding message to test %d", msg.getMessageId(), testAddressIndex));
                    }
                    testProcessorManager.processOnTest(response, fromSimulatorMessage(msg), msg.getSource(), testAddressIndex);
                }
                ctx.writeAndFlush(response);
            }
        });
    }
}
