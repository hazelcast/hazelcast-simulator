package com.hazelcast.simulator.protocol.operation;

public class StopTestOperation implements SimulatorOperation {

    private final String testId;

    public StopTestOperation(String testId) {
        this.testId = testId;
    }

    public String getTestId() {
        return testId;
    }
}
