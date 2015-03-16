package com.hazelcast.simulator.worker.commands;

public class GetBenchmarkResultsCommand extends Command {

    private final String testId;

    public GetBenchmarkResultsCommand(String testId) {
        this.testId = testId;
    }

    public String getTestId() {
        return testId;
    }

    @Override
    public String toString() {
        return "GetBenchmarkResults{}";
    }
}
