package com.hazelcast.stabilizer.worker.testcommands;

public class RunCommand extends TestCommand {

    public static final long serialVersionUID = 0l;

    public String testId;
    public boolean clientOnly = false;

    public RunCommand(String testId) {
        this.testId = testId;
    }

    @Override
    public String toString() {
        return "RunCommand{" +
                "testId='" + testId + '\'' +
                ", clientOnly=" + clientOnly +
                '}';
    }
}
