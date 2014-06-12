package com.hazelcast.stabilizer.worker.testcommands;

/**
 * Checks if there currently is a command running.
 */
public class DoneCommand extends TestCommand {

    public final String testId;

    public DoneCommand(String testId) {
        if(testId == null){
            throw new NullPointerException("testId can't be null");
        }
        this.testId = testId;
    }

    @Override
    public String toString() {
        return "DoneCommand{" +
                "testId='" + testId + '\'' +
                '}';
    }
}
