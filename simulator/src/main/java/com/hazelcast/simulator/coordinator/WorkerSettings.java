package com.hazelcast.simulator.coordinator;

import com.hazelcast.simulator.agent.workerjvm.WorkerJvmSettings;
import com.hazelcast.simulator.worker.WorkerType;

import java.io.Serializable;

/**
 * Holds the startup settings for a single Simulator Worker.
 */
public class WorkerSettings implements Serializable {

    private final int workerIndex;
    private final WorkerType type;
    private final WorkerJvmSettings workerJvmSettings;

    public WorkerSettings(int workerIndex, WorkerType type, WorkerJvmSettings workerJvmSettings) {
        this.workerIndex = workerIndex;
        this.type = type;
        this.workerJvmSettings = workerJvmSettings;
    }

    public int getWorkerIndex() {
        return workerIndex;
    }

    public WorkerType getType() {
        return type;
    }

    public WorkerJvmSettings getWorkerJvmSettings() {
        return workerJvmSettings;
    }
}
