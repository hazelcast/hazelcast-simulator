package com.hazelcast.simulator.protocol.operation;

/**
 * Stops the {@link com.hazelcast.simulator.test.TestPhase#RUN} of a Simulator test.
 */
public class StopTestOperation implements SimulatorOperation {

    private final String testId;

    public StopTestOperation(String testId) {
        this.testId = testId;
    }

    public String getTestId() {
        return testId;
    }
}
