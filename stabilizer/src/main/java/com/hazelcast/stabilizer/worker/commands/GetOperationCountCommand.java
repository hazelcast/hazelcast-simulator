package com.hazelcast.stabilizer.worker.commands;

public class GetOperationCountCommand extends Command {

    @Override
    public boolean ignoreTimeout() {
        return true;
    }

    @Override
    public String toString() {
        return "GetOperationCountCommand{}";
    }
}
