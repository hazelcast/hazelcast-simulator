package com.hazelcast.simulator.worker;

/**
 * Defines the different types for Simulator Worker components.
 */
public enum WorkerType {

    MEMBER(MemberWorker.class.getName(), true),
    CLIENT(ClientWorker.class.getName(), false);

    private final String className;
    private final boolean isMember;

    WorkerType(String className, boolean isMember) {
        this.className = className;
        this.isMember = isMember;
    }

    public String getClassName() {
        return className;
    }

    public boolean isMember() {
        return isMember;
    }

    public String toLowerCase() {
        return name().toLowerCase();
    }
}
