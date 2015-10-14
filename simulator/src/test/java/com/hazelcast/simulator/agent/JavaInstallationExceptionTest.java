package com.hazelcast.simulator.agent;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class JavaInstallationExceptionTest {

    @Test
    public void testConstructor() {
        Exception exception = new JavaInstallationException("expected");
        assertEquals("expected", exception.getMessage());
    }

    @Test
    public void testConstructor_withCause() {
        Throwable cause = new RuntimeException();
        Exception exception = new JavaInstallationException(cause);
        assertEquals(cause, exception.getCause());
    }
}
