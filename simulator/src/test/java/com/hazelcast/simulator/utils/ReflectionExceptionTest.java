package com.hazelcast.simulator.utils;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ReflectionExceptionTest {

    @Test
    public void testConstructor() {
        Exception exception = new ReflectionException("expected");
        assertEquals("expected", exception.getMessage());
    }

    @Test
    public void testConstructor_withCause() {
        Throwable cause = new RuntimeException();
        Exception exception = new ReflectionException(cause);
        assertEquals(cause, exception.getCause());
    }

    @Test
    public void testConstructor_withMessageAndCause() throws Exception {
        Throwable cause = new RuntimeException("cause");
        Exception exception = new ReflectionException("test", cause);

        assertEquals("test", exception.getMessage());
        assertEquals(cause, exception.getCause());
    }
}
