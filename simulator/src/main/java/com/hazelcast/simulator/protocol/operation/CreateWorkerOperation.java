package com.hazelcast.simulator.protocol.operation;

import com.hazelcast.simulator.agent.workerjvm.WorkerJvmSettings;

import java.util.List;

public class CreateWorkerOperation implements SimulatorOperation {

    private final List<WorkerJvmSettings> settingsList;

    public CreateWorkerOperation(List<WorkerJvmSettings> settingsList) {
        this.settingsList = settingsList;
    }

    public List<WorkerJvmSettings> getWorkerJvmSettings() {
        return settingsList;
    }
}
