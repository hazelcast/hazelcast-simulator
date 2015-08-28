package com.hazelcast.simulator.protocol.exception;

public enum ExceptionType {

    COORDINATOR_EXCEPTION("Coordinator ran into an unhandled exception"),
    AGENT_EXCEPTION("Agent ran into an unhandled exception"),
    WORKER_EXCEPTION("Worked ran into an unhandled exception"),

    WORKER_TIMEOUT("Worker has not contacted agent for a too long period"),
    WORKER_OOM("Worker ran into an OOME"),
    WORKER_EXIT("Worker terminated with a non-zero exit code");

    private final String humanReadable;

    ExceptionType(String humanReadable) {
        this.humanReadable = humanReadable;
    }

    public String getHumanReadable() {
        return humanReadable;
    }
}
