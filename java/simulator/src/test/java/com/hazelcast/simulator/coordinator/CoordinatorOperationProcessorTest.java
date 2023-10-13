package com.hazelcast.simulator.coordinator;

import com.hazelcast.simulator.agent.messages.CreateWorkerMessage;
import com.hazelcast.simulator.coordinator.messages.FailureMessage;
import com.hazelcast.simulator.protocol.Promise;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.exception.HandleException;
import com.hazelcast.simulator.worker.messages.PerformanceStatsMessage;
import com.hazelcast.simulator.worker.performance.PerformanceStats;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CoordinatorOperationProcessorTest {

    private CoordinatorMessageHandler processor;
    private FailureCollector failureCollector;
    private PerformanceStatsCollector performanceStatsCollector;
    private SimulatorAddress address;
    private Promise promise;

    @Before
    public void before() {
        failureCollector = mock(FailureCollector.class);
        performanceStatsCollector = mock(PerformanceStatsCollector.class);
        processor = new CoordinatorMessageHandler(failureCollector, performanceStatsCollector);
        address = SimulatorAddress.fromString("A1");
        promise = mock(Promise.class);
    }

    @Test
    public void test_whenFailureOperation() throws Exception {
        FailureMessage op = mock(FailureMessage.class);

        processor.process(op, address, promise);

        verify(failureCollector).notify(op);
    }

    @Test
    public void test_whenPerformanceStatsOperation() throws Exception {
        PerformanceStatsMessage op = mock(PerformanceStatsMessage.class);
        Map<String, PerformanceStats> performanceStats = mock(Map.class);
        when(op.getPerformanceStats()).thenReturn(performanceStats);

        processor.process(op, address, promise);

        verify(performanceStatsCollector).update(address, performanceStats);
    }

    @Test(expected = HandleException.class)
    public void test_whenUnknownOperation() throws Exception {
        CreateWorkerMessage op = mock(CreateWorkerMessage.class);

        processor.process(op, address, promise);
    }
}
