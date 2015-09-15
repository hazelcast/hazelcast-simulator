package com.hazelcast.simulator.protocol.operation;

import com.hazelcast.simulator.agent.workerjvm.WorkerJvmSettings;

public class CreateWorkerOperation implements SimulatorOperation {

    private final WorkerJvmSettings workerJvmSettings;

    public CreateWorkerOperation(WorkerJvmSettings workerJvmSettings) {
        this.workerJvmSettings = workerJvmSettings;
    }

    public WorkerJvmSettings getWorkerJvmSettings() {
        return workerJvmSettings;
    }
}
