package com.hazelcast.stabilizer.worker;

/**
 * Exception thrown when a test is not valid, e.g. it has no method with a {@link com.hazelcast.stabilizer.test.annotations.Run}
 * or {@link com.hazelcast.stabilizer.test.annotations.RunWithWorker} annotation.
 */
public class IllegalTestException extends RuntimeException {

    public IllegalTestException(String message) {
        super(message);
    }
}
