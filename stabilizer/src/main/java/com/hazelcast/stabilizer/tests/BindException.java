package com.hazelcast.stabilizer.tests;

public class BindException extends RuntimeException {
    public BindException(String message) {
        super(message);
    }

    public BindException(String message, Throwable cause) {
        super(message, cause);
    }
}
