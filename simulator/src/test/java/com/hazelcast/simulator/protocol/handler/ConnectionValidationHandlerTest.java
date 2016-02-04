package com.hazelcast.simulator.protocol.handler;

import com.hazelcast.simulator.protocol.core.Response;
import com.hazelcast.simulator.protocol.core.ResponseCodec;
import com.hazelcast.simulator.protocol.core.SimulatorMessage;
import com.hazelcast.simulator.protocol.core.SimulatorMessageCodec;
import com.hazelcast.simulator.protocol.operation.IntegrationTestOperation;
import com.hazelcast.simulator.protocol.operation.OperationCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static com.hazelcast.simulator.protocol.core.SimulatorAddress.COORDINATOR;
import static com.hazelcast.simulator.protocol.operation.OperationType.INTEGRATION_TEST;
import static io.netty.buffer.Unpooled.EMPTY_BUFFER;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class ConnectionValidationHandlerTest {

    private Channel channel = new NioSocketChannel();
    private ChannelHandlerContext context = mock(ChannelHandlerContext.class);

    private ConnectionValidationHandler handler = new ConnectionValidationHandler();

    private ByteBuf buffer;

    @Before
    public void setUp() throws Exception {
        when(context.channel()).thenReturn(channel);
        when(context.pipeline()).thenReturn(mock(ChannelPipeline.class));
    }

    @After
    public void tearDown() {
        if (buffer != null) {
            buffer.release();
        }
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

        verify(context).channel();
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

        verify(context).channel();
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

        verify(context).channel();
        verify(context).pipeline();
        verify(context).fireChannelRead(eq(buffer));
        verifyNoMoreInteractions(context);
    }
}
