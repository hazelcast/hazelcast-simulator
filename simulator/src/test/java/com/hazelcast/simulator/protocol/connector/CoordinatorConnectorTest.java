package com.hazelcast.simulator.protocol.connector;

import com.hazelcast.simulator.coordinator.FailureContainer;
import com.hazelcast.simulator.coordinator.HdrHistogramContainer;
import com.hazelcast.simulator.coordinator.PerformanceStateContainer;
import com.hazelcast.simulator.coordinator.TestPhaseListeners;
import com.hazelcast.simulator.protocol.core.AddressLevel;
import com.hazelcast.simulator.protocol.core.Response;
import com.hazelcast.simulator.protocol.core.ResponseFuture;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.core.SimulatorMessage;
import com.hazelcast.simulator.protocol.core.SimulatorProtocolException;
import com.hazelcast.simulator.protocol.operation.FailureOperation;
import com.hazelcast.simulator.protocol.operation.IntegrationTestOperation;
import com.hazelcast.simulator.test.FailureType;
import com.hazelcast.simulator.utils.TestUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.HashSet;
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
import static com.hazelcast.simulator.utils.CommonUtils.await;
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

    private static final SimulatorAddress WORKER_ADDRESS = new SimulatorAddress(AddressLevel.WORKER, 1, 1, 0);
    private static final SimulatorAddress AGENT_ADDRESS = WORKER_ADDRESS.getParent();

    private ExecutorService executorService;
    private CoordinatorConnector coordinatorConnector;

    @Before
    public void setUp() {
        TestPhaseListeners testPhaseListeners = new TestPhaseListeners();
        PerformanceStateContainer performanceStateContainer = new PerformanceStateContainer();

        File outputDirectory = TestUtils.createTmpDirectory();
        HdrHistogramContainer hdrHistogramContainer = new HdrHistogramContainer(outputDirectory, performanceStateContainer);
        FailureContainer failureContainer = new FailureContainer(outputDirectory, null, new HashSet<FailureType>());

        executorService = mock(ExecutorService.class);

        coordinatorConnector = new CoordinatorConnector(failureContainer, testPhaseListeners, performanceStateContainer,
                hdrHistogramContainer, executorService);
    }

    @Test(timeout = DEFAULT_TIMEOUT)
    public void testShutdown() throws Exception {
        coordinatorConnector.shutdown();

        verify(executorService).shutdown();
        verify(executorService).awaitTermination(anyLong(), any(TimeUnit.class));
    }

    @Test(timeout = DEFAULT_TIMEOUT)
    public void testWrite_withInterruptedException() {
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

        await(latch);
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

                coordinatorConnector.onFailure(operation, true, true);
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

        coordinatorConnector.onFailure(operation, false, false);

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

        coordinatorConnector.onFailure(operation, true, true);

        assertFalse("ResponseFuture.set() was called on Agent failure", responseFuture.isDone());
    }
}
