package com.hazelcast.simulator.worker.commands;

public class GetStackTraceCommand extends Command {

    public static final long serialVersionUID = 0L;

    public String testId;

    public GetStackTraceCommand(String testId) {
        this.testId = testId;
    }

    @Override
    public String toString() {
        return "GetStackTraceCommand{"
                + "testId='" + testId + '\''
                + '}';
    }
}
