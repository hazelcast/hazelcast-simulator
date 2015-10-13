package com.hazelcast.simulator.utils;

/**
 * Thrown by failures in {@link ReflectionUtils}.
 */
public class ReflectionException extends RuntimeException {

    public ReflectionException(String message) {
        super(message);
    }

    public ReflectionException(Throwable cause) {
        super(cause);
    }

    public ReflectionException(String message, Throwable cause) {
        super(message, cause);
    }
}
