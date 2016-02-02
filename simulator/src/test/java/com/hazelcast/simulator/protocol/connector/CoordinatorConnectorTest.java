package com.hazelcast.simulator.protocol.connector;

import com.hazelcast.simulator.coordinator.FailureContainer;
import com.hazelcast.simulator.coordinator.PerformanceStateContainer;
import com.hazelcast.simulator.coordinator.TestHistogramContainer;
import com.hazelcast.simulator.coordinator.TestPhaseListenerContainer;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CoordinatorConnectorTest {

    private ExecutorService executorService;
    private CoordinatorConnector coordinatorConnector;

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

    @Test
    public void testShutdown() throws Exception {
        coordinatorConnector.shutdown();

        verify(executorService).shutdown();
        verify(executorService).awaitTermination(anyLong(), any(TimeUnit.class));
    }

    @Test
    public void testShutdown_withInterruptedException() throws Exception {
        when(executorService.awaitTermination(anyLong(), any(TimeUnit.class))).thenThrow(new InterruptedException("expected"));

        coordinatorConnector.shutdown();

        verify(executorService).shutdown();
        verify(executorService).awaitTermination(anyLong(), any(TimeUnit.class));
    }
}
