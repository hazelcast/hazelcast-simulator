package com.hazelcast.stabilizer.worker.testcommands;

public class StopTestCommand extends TestCommand {

    public static final long serialVersionUID = 0l;

    public long timeoutMs;

    @Override
    public String toString() {
        return "StopTestCommand{" +
                "timeoutMs=" + timeoutMs +
                '}';
    }
}