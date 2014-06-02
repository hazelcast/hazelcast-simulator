package com.hazelcast.stabilizer.worker.testcommands;

public class RunCommand extends TestCommand {

    public static final long serialVersionUID = 0l;

    public boolean clientOnly = false;

    @Override
    public String toString() {
        return "RunCommand{" +
                "clientOnly=" + clientOnly +
                '}';
    }
}
