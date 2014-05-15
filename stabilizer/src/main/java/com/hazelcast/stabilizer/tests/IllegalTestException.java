package com.hazelcast.stabilizer.tests;

/**
 * Exception thrown when a Test is not valid, e.g. it has no method with a @Run annotation.
 */
public class IllegalTestException extends RuntimeException {

    public IllegalTestException(String message) {
        super(message);
    }
}
