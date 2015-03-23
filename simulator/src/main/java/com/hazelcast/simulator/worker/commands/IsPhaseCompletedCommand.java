package com.hazelcast.simulator.worker.commands;

/**
 * Checks if a test-phase on a member has completed for a given test.
 */
public class IsPhaseCompletedCommand extends Command {

    public final String testId;

    public IsPhaseCompletedCommand(String testId) {
        if (testId == null) {
            throw new NullPointerException("testId can't be null");
        }
        this.testId = testId;
    }

    @Override
    public String toString() {
        return "IsPhaseCompletedCommand{"
                + "testId='" + testId + '\''
                + '}';
    }
}
