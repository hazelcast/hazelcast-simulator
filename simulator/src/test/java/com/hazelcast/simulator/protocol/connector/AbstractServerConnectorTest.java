package com.hazelcast.simulator.protocol.connector;

import com.hazelcast.simulator.protocol.core.AddressLevel;
import com.hazelcast.simulator.protocol.core.ResponseFuture;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.group.ChannelGroup;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AbstractServerConnectorTest {

    private ExecutorService executorService;
    private TestServerConnector testServerConnector;

    @Before
    public void setUp() {
        executorService = mock(ExecutorService.class);
        ConcurrentMap<String, ResponseFuture> futureMap = new ConcurrentHashMap<String, ResponseFuture>();
        SimulatorAddress address = new SimulatorAddress(AddressLevel.WORKER, 1, 1, 0);

        testServerConnector = new TestServerConnector(futureMap, address, 9000, executorService);
        testServerConnector.start();
    }

    @Test
    public void testShutdown() throws Exception {
        testServerConnector.shutdown();

        verify(executorService).shutdown();
        verify(executorService).awaitTermination(anyLong(), any(TimeUnit.class));
    }

    @Test
    public void testShutdown_withInterruptedException() throws Exception {
        when(executorService.awaitTermination(anyLong(), any(TimeUnit.class))).thenThrow(new InterruptedException("expected"));

        testServerConnector.shutdown();

        verify(executorService).shutdown();
        verify(executorService).awaitTermination(anyLong(), any(TimeUnit.class));
    }

    private class TestServerConnector extends AbstractServerConnector {

        TestServerConnector(ConcurrentMap<String, ResponseFuture> futureMap, SimulatorAddress localAddress, int port,
                            ExecutorService executorService) {
            super(futureMap, localAddress, port, executorService);
        }

        @Override
        void configureServerPipeline(ChannelPipeline pipeline, ServerConnector serverConnector) {
        }

        @Override
        void connectorShutdown() {
        }

        @Override
        ChannelGroup getChannelGroup() {
            return null;
        }
    }
}
