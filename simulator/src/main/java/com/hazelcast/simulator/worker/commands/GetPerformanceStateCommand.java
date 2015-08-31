package com.hazelcast.simulator.worker.commands;

public class GetPerformanceStateCommand extends Command {

    @Override
    public boolean ignoreTimeout() {
        return true;
    }

    @Override
    public String toString() {
        return "GetPerformanceStateCommand{}";
    }
}
