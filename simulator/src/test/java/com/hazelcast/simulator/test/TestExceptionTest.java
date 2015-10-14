package com.hazelcast.simulator.test;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestExceptionTest {

    @Test
    public void testConstructor_withCause() {
        RuntimeException cause = new RuntimeException();
        TestException exception = new TestException(cause);

        assertEquals(cause, exception.getCause());
    }

    @Test
    public void testConstructor_withMessage() {
        TestException exception = new TestException("cause");

        assertEquals("cause", exception.getMessage());
    }

    @Test
    public void testConstructor_withMessageFormat_singleArgument() {
        TestException exception = new TestException("cause %d", 1);

        assertEquals("cause 1", exception.getMessage());
    }

    @Test
    public void testConstructor_withMessageFormat_multipleArguments() {
        TestException exception = new TestException("cause %d %d %s", 1, 2, "3");

        assertEquals("cause 1 2 3", exception.getMessage());
    }

    @Test
    public void testConstructor_withMessageFormat_withException() {
        Throwable cause = new RuntimeException();
        TestException exception = new TestException("cause %d %d %s", 1, 2, "3", cause);

        assertEquals("cause 1 2 3", exception.getMessage());
        assertEquals(cause, exception.getCause());
    }
}
