package com.hazelcast.simulator.protocol.connector;

import com.hazelcast.simulator.protocol.core.AddressLevel;
import com.hazelcast.simulator.protocol.core.ResponseFuture;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

public class ClientConnectorTest {

    private ClientConnector clientConnector;

    private Bootstrap bootStrap;
    private ChannelFuture future;
    private Channel channel;

    @Before
    public void setUp() {
        EventLoopGroup eventLoopGroup = mock(EventLoopGroup.class);
        ConcurrentMap<String, ResponseFuture> futureMap = new ConcurrentHashMap<String, ResponseFuture>();
        SimulatorAddress localAddress = new SimulatorAddress(AddressLevel.AGENT, 1, 0, 0);
        SimulatorAddress remoteAddress = localAddress.getChild(1);

        clientConnector = new ClientConnector(new TestClientPipelineConfigurator(), eventLoopGroup, futureMap, localAddress,
                remoteAddress, 1, "localhost", 10023, false);

        bootStrap = mock(Bootstrap.class);
        channel = mock(Channel.class);
        when(channel.closeFuture()).thenReturn(mock(ChannelFuture.class));

        future = mock(ChannelFuture.class);
        when(future.syncUninterruptibly()).thenReturn(future);
        when(future.channel()).thenReturn(channel);
    }

    @Test
    public void testConnect() {
        when(bootStrap.connect()).thenReturn(future);
        when(future.isSuccess()).thenReturn(true);

        clientConnector.connect(bootStrap, 5, 3);

        verify(bootStrap, times(1)).connect();
        verifyNoMoreInteractions(bootStrap);

        verify(future, times(1)).syncUninterruptibly();
        verify(future, times(1)).isSuccess();
        verify(future, times(1)).channel();
        verifyNoMoreInteractions(future);

        verify(channel, times(1)).writeAndFlush(any(Object.class));
        verify(channel, atLeastOnce()).closeFuture();
        verifyNoMoreInteractions(channel);
    }

    @Test
    public void testConnect_withConnectionSuccessReturnsFalseOnce() {
        when(bootStrap.connect()).thenReturn(future);
        when(future.isSuccess()).thenReturn(false).thenReturn(true);

        clientConnector.connect(bootStrap, 5, 3);

        verify(bootStrap, times(2)).connect();
        verifyNoMoreInteractions(bootStrap);

        verify(future, times(2)).syncUninterruptibly();
        verify(future, times(2)).isSuccess();
        verify(future, times(2)).channel();
        verifyNoMoreInteractions(future);

        verify(channel, times(1)).close();
        verify(channel, times(1)).writeAndFlush(any(Object.class));

        verify(channel, atLeastOnce()).closeFuture();
        verifyNoMoreInteractions(channel);
    }

    @Test
    public void testConnect_withConnectionThrowsExceptionOnce() {
        when(bootStrap.connect()).thenThrow(new RuntimeException("expected")).thenReturn(future);
        when(future.isSuccess()).thenReturn(true);

        clientConnector.connect(bootStrap, 5, 3);

        verify(bootStrap, times(2)).connect();
        verifyNoMoreInteractions(bootStrap);

        verify(future, times(1)).syncUninterruptibly();
        verify(future, times(1)).isSuccess();
        verify(future, times(1)).channel();
        verifyNoMoreInteractions(future);

        verify(channel, times(1)).writeAndFlush(any(Object.class));
        verify(channel, atLeastOnce()).closeFuture();
        verifyNoMoreInteractions(channel);
    }

    @Test
    public void testConnect_withConnectionThrowsException() {
        when(bootStrap.connect()).thenThrow(new RuntimeException("expected"));

        try {
            clientConnector.connect(bootStrap, 5, 3);
            fail("Expected RuntimeException");
        } catch (RuntimeException e) {
            assertEquals("expected", e.getMessage());
        }

        verify(bootStrap, times(3)).connect();
        verifyNoMoreInteractions(bootStrap);

        verifyZeroInteractions(future);
        verifyZeroInteractions(channel);
    }

    private class TestClientPipelineConfigurator implements ClientPipelineConfigurator {

        @Override
        public void configureClientPipeline(ChannelPipeline pipeline, SimulatorAddress remoteAddress,
                                            ConcurrentMap<String, ResponseFuture> futureMap) {
        }
    }
}
