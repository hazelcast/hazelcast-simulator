package com.hazelcast.simulator.protocol.operation;

/**
 * Starts the {@link com.hazelcast.simulator.test.TestPhase#RUN} of a Simulator test.
 */
public class StartTestOperation implements SimulatorOperation {

    private final String testId;
    private final boolean isPassiveMember;

    public StartTestOperation(String testId, boolean isPassiveMember) {
        this.testId = testId;
        this.isPassiveMember = isPassiveMember;
    }

    public String getTestId() {
        return testId;
    }

    public boolean isPassiveMember() {
        return isPassiveMember;
    }
}
