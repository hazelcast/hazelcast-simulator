package com.hazelcast.stabilizer.worker.testcommands;

public class StopTestCommand extends TestCommand {

    public static final long serialVersionUID = 0l;

    public String testId;
    public long timeoutMs;

    public StopTestCommand(String testId) {
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