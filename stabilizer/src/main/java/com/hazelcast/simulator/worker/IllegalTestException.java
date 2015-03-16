package com.hazelcast.simulator.worker;

/**
 * Exception thrown when a test is not valid, e.g. it has no method with a {@link com.hazelcast.simulator.test.annotations.Run}
 * or {@link com.hazelcast.simulator.test.annotations.RunWithWorker} annotation.
 */
public class IllegalTestException extends RuntimeException {

    public IllegalTestException(String message) {
        super(message);
    }
}
