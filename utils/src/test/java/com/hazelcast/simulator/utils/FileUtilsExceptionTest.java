package com.hazelcast.simulator.utils;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class FileUtilsExceptionTest {

    @Test
    public void testConstructor() {
        Exception exception = new FileUtilsException("expected");
        assertEquals("expected", exception.getMessage());
    }

    @Test
    public void testConstructor_withCause() {
        Throwable cause = new RuntimeException();
        Exception exception = new FileUtilsException(cause);
        assertEquals(cause, exception.getCause());
    }

    @Test
    public void testConstructor_withMessageAndCause() throws Exception {
        Throwable cause = new RuntimeException("cause");
        Exception exception = new FileUtilsException("test", cause);

        assertEquals("test", exception.getMessage());
        assertEquals(cause, exception.getCause());
    }
}
