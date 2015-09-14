package com.hazelcast.simulator.coordinator;

import com.hazelcast.simulator.agent.workerjvm.WorkerJvmSettings;
import com.hazelcast.simulator.worker.WorkerType;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The layout of workers for a given Simulator Agent.
 */
public final class AgentMemberLayout {

    private List<WorkerSettings> workerSettings = new ArrayList<WorkerSettings>();
    private final AtomicInteger currentWorkerIndex = new AtomicInteger();

    private final String publicAddress;
    private AgentMemberMode agentMemberMode;

    public AgentMemberLayout(String publicAddress, AgentMemberMode agentMemberMode) {
        this.publicAddress = publicAddress;
        this.agentMemberMode = agentMemberMode;
    }

    public String getPublicAddress() {
        return publicAddress;
    }

    public void setAgentMemberMode(AgentMemberMode agentMemberMode) {
        this.agentMemberMode = agentMemberMode;
    }

    public AgentMemberMode getAgentMemberMode() {
        return agentMemberMode;
    }

    public void addWorker(WorkerType type, WorkerJvmSettings settings) {
        workerSettings.add(new WorkerSettings(currentWorkerIndex.incrementAndGet(), type, settings));
    }

    public List<WorkerSettings> getWorkerSettings() {
        return workerSettings;
    }

    public int getCount(WorkerType type) {
        int count = 0;
        for (WorkerSettings workerSettings : this.workerSettings) {
            if (workerSettings.getType() == type) {
                count++;
            }
        }
        return count;
    }
}
