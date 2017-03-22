package com.hazelcast.simulator.agent;

import com.hazelcast.simulator.agent.operations.CreateWorkerOperation;
import com.hazelcast.simulator.agent.operations.StartTimeoutDetectionOperation;
import com.hazelcast.simulator.agent.operations.StopTimeoutDetectionOperation;
import com.hazelcast.simulator.agent.workerprocess.WorkerParameters;
import com.hazelcast.simulator.agent.workerprocess.WorkerProcessFailureMonitor;
import com.hazelcast.simulator.agent.workerprocess.WorkerProcessManager;
import com.hazelcast.simulator.protocol.Promise;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.exception.ProcessException;
import com.hazelcast.simulator.worker.operations.CreateTestOperation;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class AgentOperationProcessorTest {

    private AgentOperationProcessor processor;
    private WorkerProcessManager processManager;
    private WorkerProcessFailureMonitor failureMonitor;
    private Promise promise;
    private SimulatorAddress source;

    @Before
    public void before() {
        processManager = mock(WorkerProcessManager.class);
        failureMonitor = mock(WorkerProcessFailureMonitor.class);
        processor = new AgentOperationProcessor(processManager, failureMonitor);
        promise = mock(Promise.class);
        source = SimulatorAddress.coordinatorAddress();
    }

    @Test
    public void testCreateWorkerOperation() throws Exception {
        CreateWorkerOperation op = new CreateWorkerOperation(new ArrayList<WorkerParameters>(), 1);

        processor.process(op, source, promise);

        verify(processManager).launch(op, promise);
    }

    @Test
    public void testStartTimeoutDetectionOperation() throws Exception {
        StartTimeoutDetectionOperation op = new StartTimeoutDetectionOperation();

        processor.process(op, source, promise);

        verify(failureMonitor).startTimeoutDetection();
    }

    @Test
    public void testStopTimeoutDetectionOperation() throws Exception {
        StopTimeoutDetectionOperation op = new StopTimeoutDetectionOperation();

        processor.process(op, source, promise);

        verify(failureMonitor).stopTimeoutDetection();
    }

    @Test(expected = ProcessException.class)
    public void testUnknownOperation() throws Exception {
        CreateTestOperation op = mock(CreateTestOperation.class);

        processor.process(op, source, promise);
    }
}
