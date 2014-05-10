package com.hazelcast.stabilizer.tests;

public class TestFailureException extends RuntimeException {

    public TestFailureException() {
    }

    public TestFailureException(String s) {
        super(s);
    }

    public TestFailureException(String s, Throwable throwable) {
        super(s, throwable);
    }

    public TestFailureException(Throwable throwable) {
        super(throwable);
    }
}
