package com.hazelcast.simulator.protocol.operation;

import com.hazelcast.simulator.common.TestCase;
import com.hazelcast.simulator.common.TestSuite;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.test.TestException;
import org.junit.Before;
import org.junit.Test;

import static com.hazelcast.simulator.common.FailureType.WORKER_EXCEPTION;
import static com.hazelcast.simulator.protocol.core.AddressLevel.AGENT;
import static com.hazelcast.simulator.protocol.core.AddressLevel.WORKER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class FailureOperationTest {

    private static final String TEST_ID = "ExceptionOperationTest";

    private SimulatorAddress workerAddress = new SimulatorAddress(WORKER, 1, 1, 0);
    private SimulatorAddress agentAddress = new SimulatorAddress(AGENT, 2, 0, 0);

    private TestException cause;
    private TestSuite testSuite;

    private FailureOperation operation;
    private FailureOperation fullOperation;

    @Before
    public void setUp() {
        testSuite = new TestSuite();

        cause = new TestException("expected exception");
        operation = new FailureOperation("FailureOperationTest", WORKER_EXCEPTION, workerAddress, null, cause);
        fullOperation = new FailureOperation("FailureOperationTest", WORKER_EXCEPTION, workerAddress, null, "127.0.0.1:5701",
                "C_A1_W1-member", TEST_ID, testSuite, null);
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
        operation = new FailureOperation("FailureOperationTest", WORKER_EXCEPTION, null, null, cause);

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
        operation = new FailureOperation("FailureOperationTest", WORKER_EXCEPTION, null, agentAddress.toString(), cause);

        String log = operation.getLogMessage(5);

        assertNotNull(log);
        assertTrue(log.contains("Failure #5"));
        assertTrue(log.contains(agentAddress.toString()));
    }

    @Test
    public void testGetFileMessage_withTestCase() {
        testSuite.addTest(new TestCase(TEST_ID));

        String message = fullOperation.getFileMessage();

        assertNotNull(message);
        assertTrue(message.contains(workerAddress.toString()));
    }

    @Test
    public void testGetFileMessage_whenTestIdIsUnknown() {
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
