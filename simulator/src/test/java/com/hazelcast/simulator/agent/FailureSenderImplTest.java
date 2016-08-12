package com.hazelcast.simulator.agent;

import com.hazelcast.simulator.agent.workerprocess.WorkerProcess;
import com.hazelcast.simulator.common.TestSuite;
import com.hazelcast.simulator.protocol.connector.AgentConnector;
import com.hazelcast.simulator.protocol.core.AddressLevel;
import com.hazelcast.simulator.protocol.core.Response;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.core.SimulatorProtocolException;
import com.hazelcast.simulator.protocol.operation.SimulatorOperation;
import org.junit.Before;
import org.junit.Test;

import static com.hazelcast.simulator.common.FailureType.WORKER_EXCEPTION;
import static com.hazelcast.simulator.common.FailureType.WORKER_FINISHED;
import static com.hazelcast.simulator.protocol.core.ResponseType.FAILURE_COORDINATOR_NOT_FOUND;
import static com.hazelcast.simulator.protocol.core.ResponseType.SUCCESS;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FailureSenderImplTest {

    private static final String FAILURE_MESSAGE = "failure message";
    private static final String SESSION_ID = "FailureSenderImplTest";
    private static final String CAUSE = "any stacktrace";

    private SimulatorAddress workerAddress;
    private WorkerProcess workerProcess;

    private AgentConnector agentConnector;

    private FailureSenderImpl failureSender;

    @Before
    public void setUp() {
        workerAddress = new SimulatorAddress(AddressLevel.WORKER, 1, 1, 0);
        workerProcess = new WorkerProcess(workerAddress, workerAddress.toString(), null);

        Response response = new Response(1, SimulatorAddress.COORDINATOR, workerAddress, SUCCESS);

        agentConnector = mock(AgentConnector.class);
        when(agentConnector.write(any(SimulatorAddress.class), any(SimulatorOperation.class))).thenReturn(response);

        TestSuite testSuite = new TestSuite();

        failureSender = new FailureSenderImpl("127.0.0.1", agentConnector);
        failureSender.setTestSuite(testSuite);
    }

    @Test
    public void testSendFailureOperation() {
        boolean success = failureSender.sendFailureOperation(FAILURE_MESSAGE, WORKER_EXCEPTION, workerProcess, SESSION_ID, CAUSE);

        assertTrue(success);
    }

    @Test
    public void testSendFailureOperation_withWorkerFinished() {
        boolean success = failureSender.sendFailureOperation(FAILURE_MESSAGE, WORKER_FINISHED, workerProcess, SESSION_ID, CAUSE);

        assertTrue(success);
    }

    @Test
    public void testSendFailureOperation_withFailureResponse() {
        Response failureResponse = new Response(1, SimulatorAddress.COORDINATOR, workerAddress, FAILURE_COORDINATOR_NOT_FOUND);
        when(agentConnector.write(any(SimulatorAddress.class), any(SimulatorOperation.class))).thenReturn(failureResponse);

        boolean success = failureSender.sendFailureOperation(FAILURE_MESSAGE, WORKER_FINISHED, workerProcess, SESSION_ID, CAUSE);

        assertFalse(success);
    }

    @Test
    public void testSendFailureOperation_withProtocolException() {
        when(agentConnector.write(any(SimulatorAddress.class), any(SimulatorOperation.class)))
                .thenThrow(new SimulatorProtocolException("expected exception"));

        boolean success = failureSender.sendFailureOperation(FAILURE_MESSAGE, WORKER_FINISHED, workerProcess, SESSION_ID, CAUSE);

        assertFalse(success);
    }
}
