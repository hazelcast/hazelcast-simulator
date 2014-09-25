package com.hazelcast.stabilizer.coordinator;

import com.hazelcast.stabilizer.agent.workerjvm.WorkerJvmSettings;

/**
 * THe layout (so number of clients/servers) for a given agent.
 */
public class AgentMemberLayout {

    public WorkerJvmSettings memberSettings;
    public WorkerJvmSettings clientSettings;
    public AgentMemberMode agentMemberMode;
    public String publicIp;

    public AgentMemberLayout(WorkerJvmSettings workerJvmSettings) {
        memberSettings = new WorkerJvmSettings(workerJvmSettings);
        memberSettings.clientWorkerCount = 0;
        memberSettings.memberWorkerCount = 0;

        clientSettings = new WorkerJvmSettings(workerJvmSettings);
        clientSettings.clientWorkerCount = 0;
        clientSettings.memberWorkerCount = 0;
    }
}

