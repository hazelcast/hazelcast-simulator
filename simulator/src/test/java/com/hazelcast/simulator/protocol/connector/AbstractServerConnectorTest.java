package com.hazelcast.simulator.protocol.connector;

import com.hazelcast.simulator.protocol.core.AddressLevel;
import com.hazelcast.simulator.protocol.core.Response;
import com.hazelcast.simulator.protocol.core.ResponseFuture;
import com.hazelcast.simulator.protocol.core.ResponseType;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.operation.IntegrationTestOperation;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.group.ChannelGroup;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.simulator.protocol.core.ResponseFuture.getMessageIdFromFutureKey;
import static com.hazelcast.simulator.protocol.core.ResponseType.EXCEPTION_DURING_OPERATION_EXECUTION;
import static com.hazelcast.simulator.protocol.core.ResponseType.SUCCESS;
import static com.hazelcast.simulator.protocol.core.SimulatorAddress.COORDINATOR;
import static com.hazelcast.simulator.utils.CommonUtils.sleepMillis;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AbstractServerConnectorTest {

    private static final int PORT = 10000 + new Random().nextInt(1000);
    private static final int THREAD_POOL_SIZE = 3;
    private static final IntegrationTestOperation DEFAULT_OPERATION = new IntegrationTestOperation();

    private boolean shutdownAfterTest = true;

    private SimulatorAddress connectorAddress;
    private ConcurrentMap<String, ResponseFuture> futureMap;
    private ExecutorService executorService;

    private TestServerConnector testServerConnector;

    @Before
    public void setUp() {
        futureMap = new ConcurrentHashMap<String, ResponseFuture>();
        connectorAddress = new SimulatorAddress(AddressLevel.WORKER, 1, 1, 0);
        executorService = mock(ExecutorService.class);

        testServerConnector = new TestServerConnector(futureMap, connectorAddress, PORT, THREAD_POOL_SIZE, executorService);
    }

    @After
    public void tearDown() {
        if (shutdownAfterTest) {
            testServerConnector.shutdown();
        }
    }

    @Test
    public void testShutdown() throws Exception {
        shutdownAfterTest = false;
        testServerConnector.start();

        testServerConnector.shutdown();

        verify(executorService).shutdown();
        verify(executorService).awaitTermination(anyLong(), any(TimeUnit.class));
    }

    @Test
    public void testShutdown_withInterruptedException() throws Exception {
        shutdownAfterTest = false;
        when(executorService.awaitTermination(anyLong(), any(TimeUnit.class))).thenThrow(new InterruptedException("expected"));
        testServerConnector.start();

        testServerConnector.shutdown();

        verify(executorService).shutdown();
        verify(executorService).awaitTermination(anyLong(), any(TimeUnit.class));
    }

    @Test
    public void testShutdown_withMessageOnQueue() throws Exception {
        shutdownAfterTest = false;
        testServerConnector.start();

        testServerConnector.submit(COORDINATOR, DEFAULT_OPERATION);
        ResponseFuture future = testServerConnector.submit(COORDINATOR, DEFAULT_OPERATION);

        Thread responseSetter = new Thread() {
            @Override
            public void run() {
                sleepMillis(300);
                setResponse(SUCCESS, 2);
            }
        };

        responseSetter.start();
        testServerConnector.shutdown();
        responseSetter.join();

        Response response = future.get();
        assertEquals(SUCCESS, response.getFirstErrorResponseType());
    }

    @Test
    public void testGetMessageQueueSizeInternal() throws Exception {
        testServerConnector.submit(COORDINATOR, DEFAULT_OPERATION);
        testServerConnector.submit(COORDINATOR, DEFAULT_OPERATION);
        ResponseFuture future = testServerConnector.submit(COORDINATOR, DEFAULT_OPERATION);

        assertEquals(3, testServerConnector.getMessageQueueSizeInternal());

        testServerConnector.start();
        setResponse(SUCCESS, 3);

        Response response = future.get();
        assertEquals(SUCCESS, response.getFirstErrorResponseType());
    }

    @Test
    public void testSubmit_withFailureResponse() throws Exception {
        testServerConnector.start();

        ResponseFuture future = testServerConnector.submit(COORDINATOR, DEFAULT_OPERATION);
        setResponse(EXCEPTION_DURING_OPERATION_EXECUTION, 1);
        Response response = future.get();

        assertEquals(EXCEPTION_DURING_OPERATION_EXECUTION, response.getFirstErrorResponseType());
    }

    private void setResponse(ResponseType responseType, int expectedMessageCount) {
        int responseSetCounter = 0;
        int tries = 0;
        do {
            for (Map.Entry<String, ResponseFuture> entry : futureMap.entrySet()) {
                long messageId = getMessageIdFromFutureKey(entry.getKey());
                Response response = new Response(messageId, connectorAddress, COORDINATOR, responseType);
                entry.getValue().set(response);
                responseSetCounter++;
            }
            sleepMillis(50);
        } while (responseSetCounter < expectedMessageCount && tries++ < 50);
    }

    private class TestServerConnector extends AbstractServerConnector {

        private final ChannelGroup channelGroup = mock(ChannelGroup.class);

        TestServerConnector(ConcurrentMap<String, ResponseFuture> futureMap, SimulatorAddress localAddress, int port,
                            int threadPoolSize, ExecutorService executorService) {
            super(futureMap, localAddress, port, threadPoolSize, executorService);
        }

        @Override
        void configureServerPipeline(ChannelPipeline pipeline, ServerConnector serverConnector) {
        }

        @Override
        ChannelGroup getChannelGroup() {
            return channelGroup;
        }
    }
}
