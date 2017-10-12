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

import com.hazelcast.simulator.protocol.core.AddressLevel;
import com.hazelcast.simulator.protocol.core.ClientConnectorManager;
import com.hazelcast.simulator.protocol.core.Response;
import com.hazelcast.simulator.protocol.core.ResponseCodec;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ForwardToWorkerHandlerTest {

    private final AttributeKey<Integer> forwardAddressIndex = AttributeKey.valueOf("forwardAddressIndex");

    @Mock
    private Attribute<Integer> forwardAddressIndexAttribute;

    @Mock
    private ChannelHandlerContext ctx;

    private ForwardToWorkerHandler forwardToWorkerHandler;

    private ByteBuf buffer;

    @Before
    public void before() {
        when(forwardAddressIndexAttribute.get()).thenReturn(1);

        when(ctx.attr(forwardAddressIndex)).thenReturn(forwardAddressIndexAttribute);

        ClientConnectorManager clientConnectorManager = new ClientConnectorManager();

        forwardToWorkerHandler = new ForwardToWorkerHandler(SimulatorAddress.COORDINATOR, clientConnectorManager);
    }

    @After
    public void after() {
        if (buffer != null) {
            buffer.release();
        }
    }

    @Test
    public void testChannelRead0_forwardResponse_WorkerNotFound() throws Exception {
        SimulatorAddress destination = new SimulatorAddress(AddressLevel.WORKER, 1, 1, 0);
        Response response = new Response(12354, destination);

        buffer = Unpooled.buffer();
        ResponseCodec.encodeByteBuf(response, buffer);

        forwardToWorkerHandler.channelRead0(ctx, buffer);

        verify(ctx).attr(forwardAddressIndex);
        verify(ctx).writeAndFlush(any(Response.class));
        verifyNoMoreInteractions(ctx);
    }
}
