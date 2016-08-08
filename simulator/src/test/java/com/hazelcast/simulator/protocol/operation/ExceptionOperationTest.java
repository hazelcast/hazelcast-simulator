package com.hazelcast.simulator.protocol.operation;

import com.hazelcast.simulator.common.TestCase;
import com.hazelcast.simulator.test.TestException;
import org.junit.Before;
import org.junit.Test;

import static com.hazelcast.simulator.protocol.exception.ExceptionType.WORKER_EXCEPTION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class ExceptionOperationTest {

    private static final String TEST_ID = "ExceptionOperationTest";

    private TestException cause;
    private ExceptionOperation operation;

    @Before
    public void setUp() {
        cause = new TestException("expected exception");
        operation = new ExceptionOperation(WORKER_EXCEPTION.name(), "C_A1_W1", TEST_ID, cause);
    }

    @Test
    public void testGetTestId() {
        assertEquals(TEST_ID, operation.getTestId());
    }

    @Test
    public void testGetStackTrace() {
        String stacktrace = operation.getStacktrace();

        assertNotNull(stacktrace);
        assertTrue(stacktrace.contains("TestException"));
    }

    @Test
    public void testGetConsoleLog() {
        String log = operation.getConsoleLog(5L);

        assertNotNull(log);
        assertTrue(log.contains("Failure #5"));
    }

    @Test
    public void testGetFileLog() {
        TestCase testCase = new TestCase(TEST_ID);

        String log = operation.getFileLog(testCase);

        assertNotNull(log);
        assertTrue(log.contains("TestCase"));
    }

    @Test
    public void testGetFileLog_whenTestCaseIsNull() {
        String log = operation.getFileLog(null);

        assertNotNull(log);
        assertTrue(log.contains("test=" + TEST_ID));
    }

    @Test
    public void testGetFileLog_whenTestIdIsNull() {
        operation = new ExceptionOperation(WORKER_EXCEPTION.name(), "C_A1_W1", null, cause);

        String log = operation.getFileLog(null);

        assertNotNull(log);
        assertTrue(log.contains("test=null"));
    }
}
