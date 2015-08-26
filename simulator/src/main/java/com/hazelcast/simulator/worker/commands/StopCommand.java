package com.hazelcast.simulator.worker.commands;

public class StopCommand extends Command {

    public static final long serialVersionUID = 0L;

    public String testId;

    public StopCommand(String testId) {
        this.testId = testId;
    }

    @Override
    public String toString() {
        return "StopCommand{"
                + "testId='" + testId + '\''
                + '}';
    }
}
