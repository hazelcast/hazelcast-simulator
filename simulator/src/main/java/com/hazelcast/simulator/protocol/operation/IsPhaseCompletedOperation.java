package com.hazelcast.simulator.protocol.operation;

import com.hazelcast.simulator.test.TestPhase;

/**
 * Checks if a {@link TestPhase} of a Simulator test is still running.
 */
public class IsPhaseCompletedOperation implements SimulatorOperation {

    private final String testId;
    private final String testPhase;

    public IsPhaseCompletedOperation(String testId, TestPhase testPhase) {
        this.testId = testId;
        this.testPhase = testPhase.name();
    }

    public String getTestId() {
        return testId;
    }

    public TestPhase getTestPhase() {
        return TestPhase.valueOf(testPhase);
    }
}
