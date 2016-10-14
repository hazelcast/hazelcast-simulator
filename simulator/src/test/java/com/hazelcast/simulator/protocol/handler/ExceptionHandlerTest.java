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
