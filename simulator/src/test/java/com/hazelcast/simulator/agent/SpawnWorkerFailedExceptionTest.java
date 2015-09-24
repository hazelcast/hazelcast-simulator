package com.hazelcast.simulator.agent;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SpawnWorkerFailedExceptionTest {

    @Test
    public void testConstructor_withMessage() throws Exception {
        Exception exception = new SpawnWorkerFailedException("test");

        assertEquals("test", exception.getMessage());
    }

    @Test
    public void testConstructor_withMessageAndCause() throws Exception {
        Exception cause = new RuntimeException("cause");
        Exception exception = new SpawnWorkerFailedException("test", cause);

        assertEquals("test", exception.getMessage());
        assertEquals(cause, exception.getCause());
    }
}
