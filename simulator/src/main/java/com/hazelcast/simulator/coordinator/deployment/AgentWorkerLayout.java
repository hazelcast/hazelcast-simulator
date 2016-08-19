/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.simulator.coordinator.deployment;

import com.hazelcast.simulator.agent.workerprocess.WorkerProcessSettings;
import com.hazelcast.simulator.coordinator.WorkerParameters;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.registry.AgentData;
import com.hazelcast.simulator.worker.WorkerType;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * The layout of Simulator Workers for a given Simulator Agent.
 */
final class AgentWorkerLayout {

    private final List<WorkerProcessSettings> workerProcessSettingsList = new ArrayList<WorkerProcessSettings>();

    private final AgentData agentData;

    private AgentWorkerMode agentWorkerMode;

    AgentWorkerLayout(AgentData agentData, AgentWorkerMode agentWorkerMode) {
        this.agentData = agentData;
        this.agentWorkerMode = agentWorkerMode;
    }

    List<WorkerProcessSettings> getWorkerProcessSettings() {
        return workerProcessSettingsList;
    }

    SimulatorAddress getSimulatorAddress() {
        return agentData.getAddress();
    }

    String getPublicAddress() {
        return agentData.getPublicAddress();
    }

    String getPrivateAddress() {
        return agentData.getPrivateAddress();
    }

    Set<String> getVersionSpecs() {
        Set<String> result = new HashSet<String>();
        for (WorkerProcessSettings workerProcessSettings : workerProcessSettingsList) {
            result.add(workerProcessSettings.getVersionSpec());
        }
        return result;
    }

    void setAgentWorkerMode(AgentWorkerMode agentWorkerMode) {
        this.agentWorkerMode = agentWorkerMode;
    }

    AgentWorkerMode getAgentWorkerMode() {
        return agentWorkerMode;
    }

    WorkerProcessSettings addWorker(WorkerType type, WorkerParameters parameters) {
        WorkerProcessSettings settings = new WorkerProcessSettings(
                agentData.getNextWorkerIndex(),
                type,
                parameters.getVersionSpec(),
                parameters.getWorkerScript(),
                parameters.getWorkerStartupTimeout(),
                parameters.getEnvironment());
        workerProcessSettingsList.add(settings);

        return settings;
    }

    void addWorker(WorkerProcessSettings settings) {
        agentData.getNextWorkerIndex();
        workerProcessSettingsList.add(settings);
    }

    int getCount(WorkerType type) {
        int count = 0;
        for (WorkerProcessSettings workerProcessSettings : workerProcessSettingsList) {
            if (workerProcessSettings.getWorkerType() == type) {
                count++;
            }
        }
        return count;
    }
}
