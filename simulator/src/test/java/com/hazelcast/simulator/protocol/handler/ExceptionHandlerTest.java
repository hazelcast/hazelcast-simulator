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

import com.hazelcast.simulator.protocol.connector.ServerConnector;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.operation.FailureOperation;
import com.hazelcast.simulator.test.TestException;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class ExceptionHandlerTest {

    private ChannelHandlerContext ctx;

    private ServerConnector serverConnector;
    private ExceptionHandler exceptionHandler;

    @Before
    public void setUp() {
        ctx = mock(ChannelHandlerContext.class);
        when(ctx.channel()).thenReturn(mock(Channel.class));

        serverConnector = mock(ServerConnector.class);
        when(serverConnector.getAddress()).thenReturn(SimulatorAddress.COORDINATOR);

        exceptionHandler = new ExceptionHandler(serverConnector);
    }

    @Test
    public void testExceptionCaught() throws Exception {
        Exception cause = new TestException("expected");

        exceptionHandler.exceptionCaught(ctx, cause);

        verify(serverConnector).getAddress();
        verify(serverConnector).submit(eq(SimulatorAddress.COORDINATOR), any(FailureOperation.class));
        verifyNoMoreInteractions(serverConnector);
    }
}
