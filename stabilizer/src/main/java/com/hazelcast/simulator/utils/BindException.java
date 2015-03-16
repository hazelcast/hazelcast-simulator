package com.hazelcast.simulator.utils;

/**
 * Exception thrown when a property can't be bound to a test.
 */
public class BindException extends RuntimeException {
    public BindException(String message) {
        super(message);
    }

    public BindException(String message, Throwable cause) {
        super(message, cause);
    }
}
