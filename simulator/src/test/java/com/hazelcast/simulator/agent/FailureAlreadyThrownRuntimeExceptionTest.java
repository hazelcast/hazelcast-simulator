package com.hazelcast.simulator.agent;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class FailureAlreadyThrownRuntimeExceptionTest {

    @Test
    public void testConstructor_withCause() throws Exception {
        Exception cause = new RuntimeException("cause");
        Exception exception = new FailureAlreadyThrownRuntimeException(cause);

        assertEquals(cause, exception.getCause());
    }
}
