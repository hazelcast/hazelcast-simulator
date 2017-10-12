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

import com.hazelcast.simulator.agent.workerprocess.WorkerProcessManager;
import com.hazelcast.simulator.protocol.core.AddressLevel;
import com.hazelcast.simulator.protocol.core.ConnectionManager;
import com.hazelcast.simulator.protocol.core.Response;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.apache.log4j.Logger;

import java.util.Iterator;

import static com.hazelcast.simulator.protocol.core.ResponseType.FAILURE_COORDINATOR_NOT_FOUND;
import static com.hazelcast.simulator.protocol.core.SimulatorMessageCodec.getMessageId;
import static com.hazelcast.simulator.protocol.core.SimulatorMessageCodec.getSourceAddress;
import static com.hazelcast.simulator.protocol.core.SimulatorMessageCodec.isSimulatorMessage;
import static java.lang.String.format;

public class ForwardToCoordinatorHandler extends SimpleChannelInboundHandler<ByteBuf> {

    private static final Logger LOGGER = Logger.getLogger(ForwardToCoordinatorHandler.class);

    private final SimulatorAddress localAddress;
    private final AddressLevel addressLevel;

    private final ConnectionManager connectionManager;
    private final WorkerProcessManager workerProcessManager;

    public ForwardToCoordinatorHandler(SimulatorAddress localAddress, ConnectionManager connectionManager,
                                       WorkerProcessManager workerProcessManager) {
        this.localAddress = localAddress;
        this.addressLevel = localAddress.getAddressLevel();

        this.connectionManager = connectionManager;
        this.workerProcessManager = workerProcessManager;
    }

    @Override
    protected void channelRead0(final ChannelHandlerContext ctx, final ByteBuf buffer) throws Exception {
        if (isSimulatorMessage(buffer)) {
            long messageId = getMessageId(buffer);
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace(format("[%d] %s %s forwarding message to parent", messageId, addressLevel, localAddress));
            }

            SimulatorAddress sourceAddress = getSourceAddress(buffer);
            workerProcessManager.updateLastSeenTimestamp(sourceAddress);

            Iterator<Channel> iterator = connectionManager.getChannels().iterator();
            if (!iterator.hasNext()) {
                ctx.writeAndFlush(new Response(messageId, sourceAddress, localAddress, FAILURE_COORDINATOR_NOT_FOUND));
                return;
            }

            buffer.retain();
            iterator.next().writeAndFlush(buffer);
        }
    }
}
