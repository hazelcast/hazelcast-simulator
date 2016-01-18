package com.hazelcast.simulator.protocol.handler;

import com.hazelcast.simulator.protocol.core.AddressLevel;
import com.hazelcast.simulator.protocol.core.ClientConnectorManager;
import com.hazelcast.simulator.protocol.core.Response;
import com.hazelcast.simulator.protocol.core.ResponseCodec;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.utils.ExecutorFactory;
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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ForwardToWorkerHandlerTest {

    private final AttributeKey<Integer> forwardAddressIndex = AttributeKey.valueOf("forwardAddressIndex");
    private final ExecutorService executorService = ExecutorFactory.createFixedThreadPool(2, "ForwardToWorkerHandlerTest");

    @Mock
    private Attribute<Integer> forwardAddressIndexAttribute;

    @Mock
    private ChannelHandlerContext ctx;

    private ForwardToWorkerHandler forwardToWorkerHandler;

    private ByteBuf buffer;

    @Before
    public void setUp() {
        when(forwardAddressIndexAttribute.get()).thenReturn(1);

        when(ctx.attr(forwardAddressIndex)).thenReturn(forwardAddressIndexAttribute);

        ClientConnectorManager clientConnectorManager = new ClientConnectorManager();

        forwardToWorkerHandler = new ForwardToWorkerHandler(SimulatorAddress.COORDINATOR, clientConnectorManager,
                executorService);
    }

    @After
    public void tearDown() throws Exception {
        if (buffer != null) {
            buffer.release();
        }

        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.SECONDS);
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
