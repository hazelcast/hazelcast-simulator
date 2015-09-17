package com.hazelcast.simulator.protocol.core;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SimulatorProtocolExceptionTest {

    @Test
    public void testConstructor() {
        SimulatorProtocolException exception = new SimulatorProtocolException("expected");
        assertEquals("expected", exception.getMessage());
    }

    @Test
    public void testConstructor_withCause() {
        Throwable cause = new RuntimeException();
        SimulatorProtocolException exception = new SimulatorProtocolException("expected", cause);
        assertEquals("expected", exception.getMessage());
        assertEquals(cause, exception.getCause());
    }
}
