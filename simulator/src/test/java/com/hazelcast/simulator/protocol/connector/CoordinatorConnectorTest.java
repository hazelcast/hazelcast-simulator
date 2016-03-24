package com.hazelcast.simulator.protocol.connector;

import com.hazelcast.simulator.coordinator.FailureContainer;
import com.hazelcast.simulator.coordinator.PerformanceStateContainer;
import com.hazelcast.simulator.coordinator.TestHistogramContainer;
import com.hazelcast.simulator.coordinator.TestPhaseListenerContainer;
import com.hazelcast.simulator.protocol.core.AddressLevel;
import com.hazelcast.simulator.protocol.core.Response;
import com.hazelcast.simulator.protocol.core.ResponseFuture;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.core.SimulatorMessage;
import com.hazelcast.simulator.protocol.core.SimulatorProtocolException;
import com.hazelcast.simulator.protocol.operation.FailureOperation;
import com.hazelcast.simulator.protocol.operation.IntegrationTestOperation;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.simulator.protocol.core.ResponseFuture.createFutureKey;
import static com.hazelcast.simulator.protocol.core.ResponseFuture.createInstance;
import static com.hazelcast.simulator.protocol.core.ResponseType.UNBLOCKED_BY_FAILURE;
import static com.hazelcast.simulator.protocol.core.SimulatorAddress.COORDINATOR;
import static com.hazelcast.simulator.test.FailureType.NETTY_EXCEPTION;
import static com.hazelcast.simulator.test.FailureType.WORKER_EXIT;
import static com.hazelcast.simulator.utils.CommonUtils.joinThread;
import static com.hazelcast.simulator.utils.CommonUtils.sleepMillis;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CoordinatorConnectorTest {

    private static final int DEFAULT_TIMEOUT = 30000;

    private ExecutorService executorService;
    private CoordinatorConnector coordinatorConnector;
    public static final SimulatorAddress WORKER_ADDRESS = new SimulatorAddress(AddressLevel.WORKER, 1, 1, 0);
    public static final SimulatorAddress AGENT_ADDRESS = WORKER_ADDRESS.getParent();

    @Before
    public void setUp() {
        TestPhaseListenerContainer testPhaseListenerContainer = new TestPhaseListenerContainer();
        PerformanceStateContainer performanceStateContainer = new PerformanceStateContainer();
        TestHistogramContainer testHistogramContainer = new TestHistogramContainer(performanceStateContainer);
        FailureContainer failureContainer = new FailureContainer("ProtocolUtil", null);
        executorService = mock(ExecutorService.class);

        coordinatorConnector = new CoordinatorConnector(failureContainer, testPhaseListenerContainer, performanceStateContainer,
                testHistogramContainer, executorService);
    }

    @Test(timeout = DEFAULT_TIMEOUT)
    public void testShutdown() throws Exception {
        coordinatorConnector.shutdown();

        verify(executorService).shutdown();
        verify(executorService).awaitTermination(anyLong(), any(TimeUnit.class));
    }

    @Test(timeout = DEFAULT_TIMEOUT)
    public void testShutdown_withInterruptedException() throws Exception {
        when(executorService.awaitTermination(anyLong(), any(TimeUnit.class))).thenThrow(new InterruptedException("expected"));

        coordinatorConnector.shutdown();

        verify(executorService).shutdown();
        verify(executorService).awaitTermination(anyLong(), any(TimeUnit.class));
    }

    @Test(timeout = DEFAULT_TIMEOUT)
    public void testWrite_withInterruptedException() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicBoolean exceptionThrown = new AtomicBoolean(false);

        String futureKey = createFutureKey(COORDINATOR, 1, AGENT_ADDRESS.getAgentIndex());
        ResponseFuture responseFuture = createInstance(coordinatorConnector.getFutureMap(), futureKey);

        ClientConnector agent = mock(ClientConnector.class);
        when(agent.writeAsync(any(SimulatorMessage.class))).thenReturn(responseFuture);

        coordinatorConnector.addAgent(1, agent);

        Thread thread = new Thread() {
            @Override
            public void run() {
                latch.countDown();
                try {
                    coordinatorConnector.write(WORKER_ADDRESS, new IntegrationTestOperation());
                } catch (SimulatorProtocolException e) {
                    exceptionThrown.set(true);
                }
            }
        };
        thread.start();

        latch.await();
        thread.interrupt();

        joinThread(thread);

        assertTrue("Expected SimulatorProtocolException to be thrown, but flag was not set", exceptionThrown.get());
    }

    @Test(timeout = DEFAULT_TIMEOUT)
    public void testFailureHandling() {
        String futureKey = createFutureKey(COORDINATOR, 1, AGENT_ADDRESS.getAgentIndex());
        ResponseFuture responseFuture = createInstance(coordinatorConnector.getFutureMap(), futureKey);

        ClientConnector agent = mock(ClientConnector.class);
        when(agent.writeAsync(any(SimulatorMessage.class))).thenReturn(responseFuture);

        coordinatorConnector.addAgent(1, agent);

        final FailureOperation operation = new FailureOperation("expected", WORKER_EXIT, WORKER_ADDRESS, AGENT_ADDRESS.toString(),
                new NullPointerException("expected"));

        Thread thread = new Thread() {
            @Override
            public void run() {
                sleepMillis(50);

                coordinatorConnector.onFailure(operation, true);
            }
        };
        thread.start();

        Response response = coordinatorConnector.write(WORKER_ADDRESS, new IntegrationTestOperation());
        joinThread(thread);

        assertEquals(1, response.getMessageId());
        assertEquals(COORDINATOR, response.getDestination());
        assertEquals(1, response.size());
        assertEquals(UNBLOCKED_BY_FAILURE, response.getFirstErrorResponseType());
    }

    @Test(timeout = DEFAULT_TIMEOUT)
    public void testFailureHandling_withNonCriticalFailure() {
        String futureKey = createFutureKey(COORDINATOR, 1, AGENT_ADDRESS.getAgentIndex());
        ResponseFuture responseFuture = createInstance(coordinatorConnector.getFutureMap(), futureKey);

        ClientConnector agent = mock(ClientConnector.class);
        when(agent.writeAsync(any(SimulatorMessage.class))).thenReturn(responseFuture);

        coordinatorConnector.addAgent(1, agent);

        final FailureOperation operation = new FailureOperation("expected", WORKER_EXIT, WORKER_ADDRESS, AGENT_ADDRESS.toString(),
                new NullPointerException("expected"));

        coordinatorConnector.onFailure(operation, false);

        assertFalse("ResponseFuture.set() was called on non-critical failure", responseFuture.isDone());
    }

    @Test(timeout = DEFAULT_TIMEOUT)
    public void testFailureHandling_withAgentFailure() {
        String futureKey = createFutureKey(COORDINATOR, 1, AGENT_ADDRESS.getAgentIndex());
        ResponseFuture responseFuture = createInstance(coordinatorConnector.getFutureMap(), futureKey);

        ClientConnector agent = mock(ClientConnector.class);
        when(agent.writeAsync(any(SimulatorMessage.class))).thenReturn(responseFuture);

        coordinatorConnector.addAgent(1, agent);

        final FailureOperation operation = new FailureOperation("expected", NETTY_EXCEPTION, null, AGENT_ADDRESS.toString(),
                new NullPointerException("expected"));

        coordinatorConnector.onFailure(operation, true);

        assertFalse("ResponseFuture.set() was called on Agent failure", responseFuture.isDone());
    }
}
