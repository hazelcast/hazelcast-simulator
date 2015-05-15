package com.hazelcast.simulator.worker.commands;

import com.hazelcast.simulator.test.TestPhase;

/**
 * Checks if a test-phase on a member has completed for a given test.
 */
public class IsPhaseCompletedCommand extends Command {

    public final String testId;
    public final TestPhase testPhase;

    public IsPhaseCompletedCommand(String testId, TestPhase testPhase) {
        if (testId == null) {
            throw new NullPointerException("testId can't be null");
        }
        if (testPhase == null) {
            throw new NullPointerException("testPhase can't be null");
        }
        this.testId = testId;
        this.testPhase = testPhase;
    }

    @Override
    public String toString() {
        return "IsPhaseCompletedCommand{"
                + "testId='" + testId + '\''
                + "testPhase=" + testPhase
                + '}';
    }
}
