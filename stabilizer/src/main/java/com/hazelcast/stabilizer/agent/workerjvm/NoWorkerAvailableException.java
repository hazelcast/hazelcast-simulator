package com.hazelcast.stabilizer.agent.workerjvm;

public class NoWorkerAvailableException extends RuntimeException {
    public NoWorkerAvailableException(String msg) {
        super(msg);
    }
}
