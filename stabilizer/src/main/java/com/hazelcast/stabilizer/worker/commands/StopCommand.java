package com.hazelcast.stabilizer.worker.commands;

public class StopCommand extends Command {

    public static final long serialVersionUID = 0l;

    public String testId;
    public long timeoutMs;

    public StopCommand(String testId) {
        this.testId = testId;
    }

    @Override
    public String toString() {
        return "StopTestCommand{" +
                "testId='" + testId + '\'' +
                ", timeoutMs=" + timeoutMs +
                '}';
    }
}