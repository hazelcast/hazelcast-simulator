package com.hazelcast.simulator.coordinator.messages;

import com.hazelcast.simulator.common.TestCase;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.test.TestException;
import org.junit.Before;
import org.junit.Test;

import static com.hazelcast.simulator.common.FailureType.WORKER_EXCEPTION;
import static com.hazelcast.simulator.protocol.core.SimulatorAddress.agentAddress;
import static com.hazelcast.simulator.protocol.core.SimulatorAddress.workerAddress;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class FailureOperationTest {

    private static final String TEST_ID = "ExceptionOperationTest";

    private SimulatorAddress workerAddress = workerAddress(1, 1);
    private SimulatorAddress agentAddress = agentAddress(2);

    private TestException cause;
    private TestCase testCase;

    private FailureMessage operation;
    private FailureMessage fullOperation;

    @Before
    public void before() {
        testCase = new TestCase(TEST_ID);
        cause = new TestException("expected exception");
        operation = new FailureMessage("FailureOperationTest", WORKER_EXCEPTION, workerAddress, null, cause);
        fullOperation = new FailureMessage("FailureOperationTest", WORKER_EXCEPTION, workerAddress, null,
                "A1_W1-member", TEST_ID, null).setTestCase(testCase);
    }

    @Test
    public void testGetType() {
        assertEquals(WORKER_EXCEPTION, operation.getType());
    }

    @Test
    public void testGetWorkerAddress() {
        assertEquals(workerAddress, operation.getWorkerAddress());
    }

    @Test
    public void testGetWorkerAddress_whenWorkerAddressIsNull() {
        operation = new FailureMessage("FailureOperationTest", WORKER_EXCEPTION, null, null, cause);

        assertNull(operation.getWorkerAddress());
    }

    @Test
    public void testGetTestId() {
        assertNull(operation.getTestId());
        assertEquals(TEST_ID, fullOperation.getTestId());
    }

    @Test
    public void testGetCause() {
        assertTrue(operation.getCause().contains("TestException"));
        assertNull(fullOperation.getCause());
    }

    @Test
    public void testGetLogMessage() {
        String log = fullOperation.getLogMessage(5);

        assertNotNull(log);
        assertTrue(log.contains("Failure #5"));
        assertTrue(log.contains(workerAddress.toString()));
        assertTrue(log.contains(TEST_ID));
    }

    @Test
    public void testGetLogMessage_whenWorkerAddressIsNull() throws Exception {
        operation = new FailureMessage("FailureOperationTest", WORKER_EXCEPTION, null, agentAddress.toString(), cause);

        String log = operation.getLogMessage(5);

        assertNotNull(log);
        assertTrue(log.contains("Failure #5"));
        assertTrue(log.contains(agentAddress.toString()));
    }

    @Test
    public void testGetFileMessage_withTestCase() {
        String message = fullOperation.getFileMessage();

        assertNotNull(message);
        assertTrue(message.contains(workerAddress.toString()));
    }

    @Test
    public void testGetFileMessage_whenTestIdIsUnknown() {
        fullOperation.setTestCase(null);

        String message = fullOperation.getFileMessage();

        assertNotNull(message);
        assertTrue(message.contains(workerAddress.toString()));
        assertTrue(message.contains("test=" + TEST_ID + " (unknown)"));
    }

    @Test
    public void testGetFileMessage_whenTestIdIsNull() throws Exception {
        String message = operation.getFileMessage();

        assertNotNull(message);
        assertTrue(message.contains(workerAddress.toString()));
        assertTrue(message.contains("test=null"));
    }
}
