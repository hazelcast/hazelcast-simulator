package com.hazelcast.simulator.agent.workerjvm;

public class NoWorkerAvailableException extends RuntimeException {
    public NoWorkerAvailableException(String msg) {
        super(msg);
    }
}
