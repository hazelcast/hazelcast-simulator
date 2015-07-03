package com.hazelcast.simulator.worker.commands;

public class RunCommand extends Command {

    public static final long serialVersionUID = 0L;

    public String testId;
    public boolean passiveMembers;

    public RunCommand(String testId) {
        this.testId = testId;
    }

    @Override
    public String toString() {
        return "RunCommand{"
                + "testId='" + testId + '\''
                + ", passiveMembers=" + passiveMembers
                + '}';
    }
}
