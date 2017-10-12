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

import com.hazelcast.simulator.protocol.core.AddressLevel;
import com.hazelcast.simulator.protocol.core.Response;
import com.hazelcast.simulator.protocol.core.ResponseType;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.core.SimulatorMessage;
import com.hazelcast.simulator.protocol.exception.ProcessException;
import com.hazelcast.simulator.protocol.processors.OperationProcessor;
import com.hazelcast.simulator.worker.Promise;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.apache.log4j.Logger;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;

import static com.hazelcast.simulator.protocol.operation.OperationCodec.fromSimulatorMessage;
import static com.hazelcast.simulator.utils.EmptyStatement.ignore;
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
    private final ExecutorService executorService;

    public MessageConsumeHandler(SimulatorAddress localAddress, OperationProcessor processor, ExecutorService executorService) {
        this.localAddress = localAddress;
        this.addressLevel = localAddress.getAddressLevel();

        this.processor = processor;
        this.executorService = executorService;
    }

    @Override
    public void channelRead0(final ChannelHandlerContext ctx, final SimulatorMessage msg) {
        final long messageId = msg.getMessageId();
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("[%d] %s %s MessageConsumeHandler is consuming message...", messageId, addressLevel,
                    localAddress));
        }

        try {
            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    Promise promise = new Promise() {
                        @Override
                        public void answer(ResponseType responseType, String payload) {
                            Response response = new Response(messageId, msg.getSource())
                                    .addPart(localAddress, responseType, payload);
                            ctx.writeAndFlush(response);
                        }
                    };

                    try {
                        processor.process(fromSimulatorMessage(msg), msg.getSource(), promise);
                    } catch (ProcessException e) {
                        promise.answer(e.getResponseType(), e.getMessage());
                    } catch (Exception e) {
                        promise.answer(ResponseType.EXCEPTION_DURING_OPERATION_EXECUTION, e.getMessage());
                    }
                }
            });
        } catch (RejectedExecutionException ignore) {
            // if the executor is terminated, we can just ignore any rejected since we are shutting down
            ignore(ignore);
        }
    }
}
