/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.simulator.coordinator;

import com.hazelcast.simulator.agent.workerjvm.WorkerJvmSettings;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.registry.AgentData;
import com.hazelcast.simulator.worker.WorkerType;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The layout of workers for a given Simulator Agent.
 */
public final class AgentMemberLayout {

    private final List<WorkerJvmSettings> workerJvmSettingsList = new ArrayList<WorkerJvmSettings>();
    private final AtomicInteger currentWorkerIndex = new AtomicInteger();

    private final AgentData agentData;

    private AgentMemberMode agentMemberMode;

    public AgentMemberLayout(AgentData agentData, AgentMemberMode agentMemberMode) {
        this.agentData = agentData;
        this.agentMemberMode = agentMemberMode;
    }

    public String getPublicAddress() {
        return agentData.getPublicAddress();
    }

    public SimulatorAddress getSimulatorAddress() {
        return agentData.getAddress();
    }

    public void setAgentMemberMode(AgentMemberMode agentMemberMode) {
        this.agentMemberMode = agentMemberMode;
    }

    public AgentMemberMode getAgentMemberMode() {
        return agentMemberMode;
    }

    public void addWorker(WorkerType type, WorkerParameters parameters) {
        workerJvmSettingsList.add(new WorkerJvmSettings(currentWorkerIndex.incrementAndGet(), type, parameters));
    }

    public List<WorkerJvmSettings> getWorkerJvmSettings() {
        return workerJvmSettingsList;
    }

    public int getCount(WorkerType type) {
        int count = 0;
        for (WorkerJvmSettings workerJvmSettings : workerJvmSettingsList) {
            if (workerJvmSettings.getWorkerType() == type) {
                count++;
            }
        }
        return count;
    }
}
