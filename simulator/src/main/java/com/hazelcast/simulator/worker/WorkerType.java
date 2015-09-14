package com.hazelcast.simulator.worker;

/**
 * Defines the different types for Simulator Worker components.
 */
public enum WorkerType {

    MEMBER(MemberWorker.class.getName()),
    CLIENT(ClientWorker.class.getName());

    private final String className;

    WorkerType(String className) {
        this.className = className;
    }

    public String getClassName() {
        return className;
    }
}
