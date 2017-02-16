/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.simulator.protocol.connector.ServerConnector;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.operation.FailureOperation;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;
import org.apache.log4j.Logger;

import java.io.IOException;

import static com.hazelcast.simulator.common.FailureType.NETTY_EXCEPTION;
import static com.hazelcast.simulator.protocol.core.SimulatorAddress.COORDINATOR;

/**
 * Handles uncaught exceptions in the channel pipeline.
 */
@ChannelHandler.Sharable
public class ExceptionHandler extends ChannelHandlerAdapter {

    private static final Logger LOGGER = Logger.getLogger(ExceptionHandler.class);

    private final SimulatorAddress workerAddress;
    private final String agentAddress;

    private final ServerConnector serverConnector;

    public ExceptionHandler(ServerConnector serverConnector) {
        SimulatorAddress localAddress = serverConnector.getAddress();
        switch (localAddress.getAddressLevel()) {
            case WORKER:
                this.workerAddress = localAddress;
                this.agentAddress = localAddress.getParent().toString();
                break;
            case AGENT:
                this.workerAddress = null;
                this.agentAddress = localAddress.toString();
                break;
            default:
                this.workerAddress = null;
                this.agentAddress = null;
        }

        this.serverConnector = serverConnector;
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        if (isConnectionResetByPeerException(cause) || isConnectionResetByPeerException(cause.getCause())) {
            return;
        }

        String message = "Caught unhandled exception in Netty pipeline in channel " + ctx.channel();
        LOGGER.error(message, cause);
        FailureOperation operation = new FailureOperation(message, NETTY_EXCEPTION, workerAddress, agentAddress, cause);
        serverConnector.submit(COORDINATOR, operation);
    }

    private boolean isConnectionResetByPeerException(Throwable cause) {
        return cause instanceof IOException && "Connection reset by peer".equals(cause.getMessage());
    }
}
