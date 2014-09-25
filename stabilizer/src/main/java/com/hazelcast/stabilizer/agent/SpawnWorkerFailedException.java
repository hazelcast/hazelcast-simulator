package com.hazelcast.stabilizer.agent;

public class SpawnWorkerFailedException extends RuntimeException {
    public SpawnWorkerFailedException(String message) {
        super(message);
    }
}
