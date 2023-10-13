package com.hazelcast.simulator.agent;

import com.hazelcast.simulator.agent.messages.CreateWorkerMessage;
import com.hazelcast.simulator.agent.messages.StartTimeoutDetectionMessage;
import com.hazelcast.simulator.agent.messages.StopTimeoutDetectionMessage;
import com.hazelcast.simulator.agent.workerprocess.WorkerParameters;
import com.hazelcast.simulator.agent.workerprocess.WorkerProcessFailureMonitor;
import com.hazelcast.simulator.agent.workerprocess.WorkerProcessManager;
import com.hazelcast.simulator.protocol.Promise;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.exception.HandleException;
import com.hazelcast.simulator.worker.messages.CreateTestMessage;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class AgentMessageProcessorTest {

    private AgentMessageHandler processor;
    private WorkerProcessManager processManager;
    private WorkerProcessFailureMonitor failureMonitor;
    private Promise promise;
    private SimulatorAddress source;

    @Before
    public void before() {
        processManager = mock(WorkerProcessManager.class);
        failureMonitor = mock(WorkerProcessFailureMonitor.class);
        processor = new AgentMessageHandler(processManager, failureMonitor);
        promise = mock(Promise.class);
        source = SimulatorAddress.coordinatorAddress();
    }

    @Test
    public void testCreateWorkerOperation() throws Exception {
        CreateWorkerMessage msg = new CreateWorkerMessage(new WorkerParameters(), 1);

        processor.process(msg, source, promise);

        verify(processManager).launch(msg, promise);
    }

    @Test
    public void testStartTimeoutDetectionOperation() throws Exception {
        StartTimeoutDetectionMessage msg = new StartTimeoutDetectionMessage();

        processor.process(msg, source, promise);

        verify(failureMonitor).startTimeoutDetection();
    }

    @Test
    public void testStopTimeoutDetectionOperation() throws Exception {
        StopTimeoutDetectionMessage msg = new StopTimeoutDetectionMessage();

        processor.process(msg, source, promise);

        verify(failureMonitor).stopTimeoutDetection();
    }

    @Test(expected = HandleException.class)
    public void testUnknownOperation() throws Exception {
        CreateTestMessage msg = mock(CreateTestMessage.class);

        processor.process(msg, source, promise);
    }
}
