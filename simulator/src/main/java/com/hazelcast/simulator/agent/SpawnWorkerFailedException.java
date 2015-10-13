package com.hazelcast.simulator.agent;

public class SpawnWorkerFailedException extends RuntimeException {

    public SpawnWorkerFailedException(String message) {
        super(message);
    }

    public SpawnWorkerFailedException(String message, Throwable cause) {
        super(message, cause);
    }
}
