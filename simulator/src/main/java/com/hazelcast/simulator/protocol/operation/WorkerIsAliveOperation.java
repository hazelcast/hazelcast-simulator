package com.hazelcast.simulator.protocol.operation;

import com.hazelcast.simulator.protocol.core.SimulatorAddress;

public class WorkerIsAliveOperation implements SimulatorOperation {

    private final SimulatorAddress source;

    public WorkerIsAliveOperation(SimulatorAddress source) {
        this.source = source;
    }

    public SimulatorAddress getSource() {
        return source;
    }
}
