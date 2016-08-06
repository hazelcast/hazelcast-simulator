package com.hazelcast.simulator.utils;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class UncheckedIOExceptionTest {

    @Test
    public void testConstructor() {
        Exception exception = new UncheckedIOException("expected");
        assertEquals("expected", exception.getMessage());
    }

    @Test
    public void testConstructor_withCause() {
        Throwable cause = new RuntimeException();
        Exception exception = new UncheckedIOException(cause);
        assertEquals(cause, exception.getCause());
    }

    @Test
    public void testConstructor_withMessageAndCause() throws Exception {
        Throwable cause = new RuntimeException("cause");
        Exception exception = new UncheckedIOException("test", cause);

        assertEquals("test", exception.getMessage());
        assertEquals(cause, exception.getCause());
    }
}
