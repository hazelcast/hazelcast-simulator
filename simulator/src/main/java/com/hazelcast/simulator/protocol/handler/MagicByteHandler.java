/*
 * Copyright (c) 2008-2014, Hazelcast, Inc. All Rights Reserved.
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

@ChannelHandler.Sharable
public class MagicByteHandler extends ChannelInboundHandlerAdapter {

    private static final int MAGIC_BYTES_REQUEST = 0xA5E1CA57;
    private static final int MAGIC_BYTES_RESPONSE = 0x3E5D0B5E;

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object obj) throws Exception {
        if (!(obj instanceof ByteBuf)) {
            return;
        }

        ByteBuf buf = (ByteBuf) obj;
        if (buf.readableBytes() < 8) {
            return;
        }

        int magic = buf.getInt(4);
        if (magic != MAGIC_BYTES_REQUEST && magic != MAGIC_BYTES_RESPONSE) {
            buf.clear();
            ctx.close();
            return;
        }

        ctx.fireChannelRead(obj);
    }
}
