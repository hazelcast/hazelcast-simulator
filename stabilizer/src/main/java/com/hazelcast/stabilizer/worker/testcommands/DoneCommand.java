package com.hazelcast.stabilizer.worker.testcommands;

/**
 * Checks if there currently is a command running.
 */
public class DoneCommand extends TestCommand {
    @Override
    public String toString() {
        return "DoneCommand";
    }
}
