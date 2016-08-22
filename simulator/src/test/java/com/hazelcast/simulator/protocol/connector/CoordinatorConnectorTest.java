package com.hazelcast.simulator.protocol.connector;

import com.hazelcast.simulator.coordinator.FailureCollector;
import com.hazelcast.simulator.coordinator.PerformanceStatsCollector;
import com.hazelcast.simulator.coordinator.TestPhaseListeners;
import com.hazelcast.simulator.protocol.core.AddressLevel;
import com.hazelcast.simulator.protocol.core.ResponseFuture;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.core.SimulatorMessage;
import com.hazelcast.simulator.protocol.core.SimulatorProtocolException;
import com.hazelcast.simulator.protocol.operation.IntegrationTestOperation;
import com.hazelcast.simulator.protocol.processors.CoordinatorOperationProcessor;
import com.hazelcast.simulator.utils.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.simulator.protocol.core.ResponseFuture.createFutureKey;
import static com.hazelcast.simulator.protocol.core.ResponseFuture.createInstance;
import static com.hazelcast.simulator.protocol.core.SimulatorAddress.COORDINATOR;
import static com.hazelcast.simulator.utils.CommonUtils.await;
import static com.hazelcast.simulator.utils.CommonUtils.joinThread;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CoordinatorConnectorTest {

    private static final int DEFAULT_TIMEOUT = 30000;

    private static final int COORDINATOR_PORT = 0;
    private static final SimulatorAddress WORKER_ADDRESS = new SimulatorAddress(AddressLevel.WORKER, 1, 1, 0);
    private static final SimulatorAddress AGENT_ADDRESS = WORKER_ADDRESS.getParent();

    private CoordinatorConnector coordinatorConnector;

    @Before
    public void setUp() {
         TestPhaseListeners testPhaseListeners = new TestPhaseListeners();
        PerformanceStatsCollector performanceStatsCollector = new PerformanceStatsCollector();

        File outputDirectory = TestUtils.createTmpDirectory();
        FailureCollector failureCollector = new FailureCollector(outputDirectory);

        CoordinatorOperationProcessor processor = new CoordinatorOperationProcessor(null, failureCollector, testPhaseListeners, performanceStatsCollector);
        coordinatorConnector = new CoordinatorConnector(processor, COORDINATOR_PORT);
        coordinatorConnector.start();
    }

    @After
    public void tearDown() {
        coordinatorConnector.shutdown();
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
}
