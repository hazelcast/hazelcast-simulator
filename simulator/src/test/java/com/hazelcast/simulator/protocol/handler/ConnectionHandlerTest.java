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

import com.hazelcast.simulator.protocol.core.ConnectionManager;
import com.hazelcast.simulator.protocol.core.Response;
import com.hazelcast.simulator.protocol.core.ResponseCodec;
import com.hazelcast.simulator.protocol.core.SimulatorMessage;
import com.hazelcast.simulator.protocol.core.SimulatorMessageCodec;
import com.hazelcast.simulator.protocol.operation.IntegrationTestOperation;
import com.hazelcast.simulator.protocol.operation.OperationCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelId;
import io.netty.channel.ChannelPipeline;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.SocketAddress;

import static com.hazelcast.simulator.protocol.core.SimulatorAddress.COORDINATOR;
import static com.hazelcast.simulator.protocol.operation.OperationType.INTEGRATION_TEST;
import static com.hazelcast.simulator.utils.CommonUtils.sleepMillis;
import static io.netty.buffer.Unpooled.EMPTY_BUFFER;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class ConnectionHandlerTest {

    private Channel channel = mock(Channel.class);
    private ChannelHandlerContext context = mock(ChannelHandlerContext.class);

    private ConnectionManager connectionManager = new ConnectionManager();

    private ConnectionHandler handler = new ConnectionHandler(connectionManager);

    private ByteBuf buffer;

    @Before
    public void before() {
        SocketAddress remoteAddress = mock(SocketAddress.class);
        when(remoteAddress.toString()).thenReturn("/127.0.0.1:50123");

        when(channel.remoteAddress()).thenReturn(remoteAddress);
        when(channel.closeFuture()).thenReturn(mock(ChannelFuture.class));
        when(channel.id()).thenReturn(mock(ChannelId.class));

        when(context.channel()).thenReturn(channel);
        when(context.pipeline()).thenReturn(mock(ChannelPipeline.class));
    }

    @After
    public void after() {
        if (buffer != null) {
            buffer.release();
        }
    }

    @Test
    public void testChannelActive() throws Exception {
        handler.acceptConnection();
        handler.channelActive(context);

        verify(context, atLeast(1)).channel();
        verifyNoMoreInteractions(context);
    }

    @Test
    public void testTimeoutInvalidConnection_whenConnectionIsValid() {
        handler.acceptConnection();
        handler.timeoutInvalidConnection(context, 0);
        sleepMillis(500);

        verify(context, atLeast(1)).channel();
        verifyNoMoreInteractions(context);
    }

    @Test
    public void testTimeoutInvalidConnection_whenConnectionIsInvalid() {
        handler.timeoutInvalidConnection(context, 0);
        sleepMillis(500);

        verify(context, atLeast(1)).channel();
        verify(context).close();
        verifyNoMoreInteractions(context);
    }

    @Test
    public void testChannelRead_noByteBuf() throws Exception {
        handler.channelRead(context, new Object());
        verifyNoMoreInteractions(context);
    }

    @Test
    public void testChannelRead_tooShortByteBuf() throws Exception {
        handler.channelRead(context, EMPTY_BUFFER);
        verifyNoMoreInteractions(context);
    }

    @Test
    public void testChannelRead_invalidByteBuf() throws Exception {
        buffer = Unpooled.buffer().capacity(12);
        buffer.writeInt(23);
        buffer.writeInt(42);
        buffer.writeInt(1234);

        handler.channelRead(context, buffer);

        verify(context, atLeast(1)).channel();
        verify(context).close();
        verifyNoMoreInteractions(context);
    }

    @Test
    public void testChannelRead_SimulatorMessage() throws Exception {
        String operationData = OperationCodec.toJson(new IntegrationTestOperation());
        SimulatorMessage message = new SimulatorMessage(COORDINATOR, COORDINATOR, 1, INTEGRATION_TEST, operationData);
        buffer = Unpooled.buffer();
        SimulatorMessageCodec.encodeByteBuf(message, buffer);

        handler.channelRead(context, buffer);

        verify(context, atLeast(1)).channel();
        verify(context).pipeline();
        verify(context).fireChannelRead(eq(buffer));
        verifyNoMoreInteractions(context);
    }

    @Test
    public void testChannelRead_Response() throws Exception {
        Response response = new Response(1, COORDINATOR);
        buffer = Unpooled.buffer();
        ResponseCodec.encodeByteBuf(response, buffer);

        handler.channelRead(context, buffer);

        verify(context, atLeast(1)).channel();
        verify(context).pipeline();
        verify(context).fireChannelRead(eq(buffer));
        verifyNoMoreInteractions(context);
    }
}
