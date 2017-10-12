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

import com.hazelcast.simulator.protocol.connector.ClientConnector;
import com.hazelcast.simulator.protocol.core.AddressLevel;
import com.hazelcast.simulator.protocol.core.ClientConnectorManager;
import com.hazelcast.simulator.protocol.core.Response;
import com.hazelcast.simulator.protocol.core.ResponseCodec;
import com.hazelcast.simulator.protocol.core.ResponseFuture;
import com.hazelcast.simulator.protocol.core.ResponseFutureListener;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.core.SimulatorMessageCodec;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.AttributeKey;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.simulator.protocol.core.ResponseCodec.isResponse;
import static com.hazelcast.simulator.protocol.core.ResponseType.FAILURE_WORKER_NOT_FOUND;
import static com.hazelcast.simulator.protocol.core.SimulatorMessageCodec.getSourceAddress;
import static com.hazelcast.simulator.protocol.core.SimulatorMessageCodec.isSimulatorMessage;
import static java.lang.String.format;

/**
 * A {@link SimpleChannelInboundHandler} to forward a received {@link ByteBuf} to a connected Simulator Worker.
 */
public class ForwardToWorkerHandler extends SimpleChannelInboundHandler<ByteBuf> {

    private static final Logger LOGGER = Logger.getLogger(ForwardToWorkerHandler.class);

    private final AttributeKey<Integer> forwardAddressIndex = AttributeKey.valueOf("forwardAddressIndex");

    private final SimulatorAddress localAddress;
    private final AddressLevel addressLevel;

    private final ClientConnectorManager clientConnectorManager;

    public ForwardToWorkerHandler(SimulatorAddress localAddress, ClientConnectorManager clientConnectorManager) {
        this.localAddress = localAddress;
        this.addressLevel = localAddress.getAddressLevel();
        this.clientConnectorManager = clientConnectorManager;
    }

    @Override
    protected void channelRead0(final ChannelHandlerContext ctx, final ByteBuf buffer) throws Exception {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("ForwardToWorkerHandler.channelRead0() %s %s", addressLevel, localAddress));
        }

        int workerAddressIndex = ctx.attr(forwardAddressIndex).get();
        if (isSimulatorMessage(buffer)) {
            forwardSimulatorMessage(ctx, buffer, workerAddressIndex);
        } else if (isResponse(buffer)) {
            forwardResponse(ctx, buffer, workerAddressIndex);
        }
    }

    private void forwardSimulatorMessage(final ChannelHandlerContext ctx, ByteBuf buffer, int workerAddressIndex) {
        final long messageId = SimulatorMessageCodec.getMessageId(buffer);

        final List<ResponseFuture> futureList = new ArrayList<ResponseFuture>();
        final Response response = new Response(messageId, getSourceAddress(buffer));
        if (workerAddressIndex == 0) {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace(format("[%d] %s forwarding message to all workers", messageId, addressLevel));
            }
            for (ClientConnector clientConnector : clientConnectorManager.getClientConnectors()) {
                buffer.retain();
                futureList.add(clientConnector.writeAsync(buffer));
            }
        } else {
            ClientConnector clientConnector = clientConnectorManager.get(workerAddressIndex);
            if (clientConnector == null) {
                LOGGER.error(format("[%d] %s Worker %d not found!", messageId, addressLevel, workerAddressIndex));
                response.addPart(localAddress, FAILURE_WORKER_NOT_FOUND);
                ctx.writeAndFlush(response);
                return;
            }
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace(format("[%d] %s forwarding message to Worker %d", messageId, addressLevel, workerAddressIndex));
            }
            buffer.retain();
            futureList.add(clientConnector.writeAsync(buffer));
        }

        // if we don't need to wait for anything, we are done.
        if (futureList.isEmpty()) {
            ctx.writeAndFlush(response);
            return;
        }

        ResponseFutureListener listener = new ResponseFutureListener() {
            private int responsesRemaining = futureList.size();

            @Override
            public void onCompletion(Response r) {
                int remaining;
                synchronized (this) {
                    responsesRemaining--;
                    remaining = responsesRemaining;
                    response.addAllParts(r);
                }

                if (remaining == 0) {
                    ctx.writeAndFlush(response);
                }
            }
        };

        for (ResponseFuture future : futureList) {
            future.addListener(listener);
        }
    }

    private void forwardResponse(ChannelHandlerContext ctx, ByteBuf buffer, int workerAddressIndex) {
        long messageId = ResponseCodec.getMessageId(buffer);

        ClientConnector clientConnector = clientConnectorManager.get(workerAddressIndex);
        if (clientConnector == null) {
            LOGGER.error(format("[%d] %s Worker %d not found!", messageId, addressLevel, workerAddressIndex));
            ctx.writeAndFlush(new Response(messageId, localAddress, localAddress, FAILURE_WORKER_NOT_FOUND));
            return;
        }

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("[%d] %s forwarding response to Worker %d", messageId, addressLevel, workerAddressIndex));
        }
        buffer.retain();
        clientConnector.forwardToChannel(buffer);
    }
}
