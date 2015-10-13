package com.hazelcast.simulator.protocol.handler;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.junit.Before;
import org.junit.Test;

import static com.hazelcast.simulator.utils.CommonUtils.rethrow;
import static com.hazelcast.simulator.utils.CommonUtils.sleepMillis;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ChannelCollectorHandlerTest {

    private ChannelCollectorHandler channelCollectorHandler = new ChannelCollectorHandler();
    private ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);

    @Before
    public void setUp() {
        when(ctx.channel()).thenReturn(new NioSocketChannel());
    }

    @Test
    public void testGetChannels() throws Exception {
        channelCollectorHandler.channelActive(ctx);

        assertEquals(1, channelCollectorHandler.getChannels().size());
    }

    @Test
    public void testGetChannels_empty() {
        assertEquals(0, channelCollectorHandler.getChannels().size());
    }

    @Test(timeout = 5000)
    public void testWaitForAtLeastOneChannel() throws Exception {
        Thread addChannelThread = new Thread() {
            @Override
            public void run() {
                sleepMillis(500);
                try {
                    channelCollectorHandler.channelActive(ctx);
                } catch (Exception e) {
                    throw rethrow(e);
                }
            }
        };
        addChannelThread.start();

        channelCollectorHandler.waitForAtLeastOneChannel();
        addChannelThread.join();
    }

    @Test(timeout = 5000)
    public void testWaitForAtLeastOneChannel_interrupted() throws Exception {
        final Thread currentThread = Thread.currentThread();
        Thread interruptThread = new Thread() {
            @Override
            public void run() {
                sleepMillis(500);
                currentThread.interrupt();
            }
        };
        interruptThread.start();

        channelCollectorHandler.waitForAtLeastOneChannel();
        interruptThread.join();
    }
}
