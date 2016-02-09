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

import com.hazelcast.simulator.protocol.core.ConnectionListener;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

/**
 * Listens for new connections and registers them in the configured {@link ConnectionListener}.
 */
public class ConnectionListenerHandler extends ChannelInboundHandlerAdapter {

    private final ConnectionListener connectionListener;

    public ConnectionListenerHandler(ConnectionListener connectionListener) {
        this.connectionListener = connectionListener;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        ctx.channel().closeFuture().addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                connectionListener.disconnected(future.channel());
            }
        });

        connectionListener.connected(ctx.channel());
    }
}
