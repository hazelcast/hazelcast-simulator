package com.hazelcast.simulator.coordinator;

import com.hazelcast.simulator.agent.operations.CreateWorkerOperation;
import com.hazelcast.simulator.coordinator.operations.FailureOperation;
import com.hazelcast.simulator.protocol.Promise;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.exception.ProcessException;
import com.hazelcast.simulator.worker.operations.PerformanceStatsOperation;
import com.hazelcast.simulator.worker.performance.PerformanceStats;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CoordinatorOperationProcessorTest {

    private CoordinatorOperationProcessor processor;
    private FailureCollector failureCollector;
    private PerformanceStatsCollector performanceStatsCollector;
    private SimulatorAddress address;
    private Promise promise;

    @Before
    public void before() {
        failureCollector = mock(FailureCollector.class);
        performanceStatsCollector = mock(PerformanceStatsCollector.class);
        processor = new CoordinatorOperationProcessor(failureCollector, performanceStatsCollector);
        address = SimulatorAddress.fromString("C_A1");
        promise = mock(Promise.class);
    }

    @Test
    public void test_whenFailureOperation() throws Exception {
        FailureOperation op = mock(FailureOperation.class);

        processor.process(op, address, promise);

        verify(failureCollector).notify(op);
    }

    @Test
    public void test_whenPerformanceStatsOperation() throws Exception {
        PerformanceStatsOperation op = mock(PerformanceStatsOperation.class);
        Map<String, PerformanceStats> performanceStats = mock(Map.class);
        when(op.getPerformanceStats()).thenReturn(performanceStats);

        processor.process(op, address, promise);

        verify(performanceStatsCollector).update(address, performanceStats);
    }

    @Test(expected = ProcessException.class)
    public void test_whenUnknownOperation() throws Exception {
        CreateWorkerOperation op = mock(CreateWorkerOperation.class);

        processor.process(op, address, promise);
    }
}
