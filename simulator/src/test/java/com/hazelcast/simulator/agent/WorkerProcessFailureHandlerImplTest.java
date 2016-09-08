package com.hazelcast.simulator.agent;

import com.hazelcast.simulator.agent.workerprocess.WorkerProcess;
import com.hazelcast.simulator.protocol.connector.AgentConnector;
import com.hazelcast.simulator.protocol.connector.AgentConnectorImpl;
import com.hazelcast.simulator.protocol.core.AddressLevel;
import com.hazelcast.simulator.protocol.core.Response;
import com.hazelcast.simulator.protocol.core.ResponseFuture;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.core.SimulatorProtocolException;
import com.hazelcast.simulator.protocol.operation.SimulatorOperation;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.simulator.common.FailureType.NETTY_EXCEPTION;
import static com.hazelcast.simulator.common.FailureType.WORKER_EXCEPTION;
import static com.hazelcast.simulator.common.FailureType.WORKER_NORMAL_EXIT;
import static com.hazelcast.simulator.protocol.core.ResponseFuture.createFutureKey;
import static com.hazelcast.simulator.protocol.core.ResponseFuture.createInstance;
import static com.hazelcast.simulator.protocol.core.ResponseType.FAILURE_COORDINATOR_NOT_FOUND;
import static com.hazelcast.simulator.protocol.core.ResponseType.SUCCESS;
import static com.hazelcast.simulator.protocol.core.ResponseType.UNBLOCKED_BY_FAILURE;
import static com.hazelcast.simulator.protocol.core.SimulatorAddress.COORDINATOR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class WorkerProcessFailureHandlerImplTest {

    private static final String FAILURE_MESSAGE = "failure message";
    private static final String SESSION_ID = "WorkerProcessFailureHandlerImplTest";
    private static final String CAUSE = "any stacktrace";

    private SimulatorAddress workerAddress;
    private WorkerProcess workerProcess;

    private AgentConnector agentConnector;

    private WorkerProcessFailureHandlerImpl failureSender;

    private ResponseFuture responseFuture;

    @Before
    public void before() {
        workerAddress = new SimulatorAddress(AddressLevel.WORKER, 2, 3, 0);
        workerProcess = new WorkerProcess(workerAddress, workerAddress.toString(), null);

        int messageId = 1;
        String futureKey = createFutureKey(COORDINATOR, messageId, workerAddress.getAddressIndex());
        ConcurrentMap<String, ResponseFuture> futureMap = new ConcurrentHashMap<String, ResponseFuture>();
        responseFuture = createInstance(futureMap, futureKey);

        Response response = new Response(messageId, COORDINATOR, workerAddress, SUCCESS);

        agentConnector = mock(AgentConnectorImpl.class);
        when(agentConnector.invoke(any(SimulatorAddress.class), any(SimulatorOperation.class))).thenReturn(response);
        when(agentConnector.getFutureMap()).thenReturn(futureMap);

        failureSender = new WorkerProcessFailureHandlerImpl("127.0.0.1", agentConnector);
    }

    @Test
    public void testSendFailureOperation() {
        boolean success = failureSender.handle(FAILURE_MESSAGE, WORKER_EXCEPTION, workerProcess, SESSION_ID, CAUSE);

        assertTrue(success);
        assertFalse(responseFuture.isDone());
    }

    @Test(timeout = 10000)
    public void testSendFailureOperation_whenWorkerIsFinished_thenUnblockResponseFutureByFailure() throws Exception {
        boolean success = failureSender.handle(FAILURE_MESSAGE, WORKER_NORMAL_EXIT, workerProcess, SESSION_ID, CAUSE);

        assertTrue(success);
        assertTrue(responseFuture.isDone());
        assertEquals(responseFuture.get().getFirstErrorResponseType(), UNBLOCKED_BY_FAILURE);
    }

    @Test
    public void testSendFailureOperation_withFailureResponse() {
        Response failureResponse = new Response(1, COORDINATOR, workerAddress, FAILURE_COORDINATOR_NOT_FOUND);
        when(agentConnector.invoke(any(SimulatorAddress.class), any(SimulatorOperation.class))).thenReturn(failureResponse);

        boolean success = failureSender.handle(FAILURE_MESSAGE, WORKER_EXCEPTION, workerProcess, SESSION_ID, CAUSE);

        assertFalse(success);
        assertFalse(responseFuture.isDone());
    }

    @Test
    public void testSendFailureOperation_withProtocolException() {
        when(agentConnector.invoke(any(SimulatorAddress.class), any(SimulatorOperation.class)))
                .thenThrow(new SimulatorProtocolException("expected exception"));

        boolean success = failureSender.handle(FAILURE_MESSAGE, NETTY_EXCEPTION, workerProcess, SESSION_ID, CAUSE);

        assertFalse(success);
        assertFalse(responseFuture.isDone());
    }
}
