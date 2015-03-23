package com.hazelcast.simulator.worker.commands;

public class StopCommand extends Command {

    public static final long serialVersionUID = 0l;

    public String testId;
    public long timeoutMs;

    public StopCommand(String testId) {
        this.testId = testId;
    }

    @Override
    public String toString() {
        return "StopCommand{"
                + "testId='" + testId + '\''
                + ", timeoutMs=" + timeoutMs
                + '}';
    }
}