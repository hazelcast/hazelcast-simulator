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
package com.hazelcast.simulator.cluster;

import com.hazelcast.simulator.agent.workerprocess.WorkerProcessSettings;
import com.hazelcast.simulator.coordinator.WorkerParameters;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.registry.AgentData;
import com.hazelcast.simulator.worker.WorkerType;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The layout of workers for a given Simulator Agent.
 */
public final class AgentWorkerLayout {

    private final List<WorkerProcessSettings> workerProcessSettingsList = new ArrayList<WorkerProcessSettings>();
    private final AtomicInteger currentWorkerIndex = new AtomicInteger();

    private final AgentData agentData;

    private AgentWorkerMode agentWorkerMode;

    AgentWorkerLayout(AgentData agentData, AgentWorkerMode agentWorkerMode) {
        this.agentData = agentData;
        this.agentWorkerMode = agentWorkerMode;
    }

    public List<WorkerProcessSettings> getWorkerJvmSettings() {
        return workerProcessSettingsList;
    }

    public SimulatorAddress getSimulatorAddress() {
        return agentData.getAddress();
    }

    public String getPublicAddress() {
        return agentData.getPublicAddress();
    }

    public String getPrivateAddress() {
        return agentData.getPrivateAddress();
    }

    public Set<String> getHazelcastVersionSpecs() {
        Set<String> hazelcastVersionSpecs = new HashSet<String>();
        for (WorkerProcessSettings workerProcessSettings : workerProcessSettingsList) {
            hazelcastVersionSpecs.add(workerProcessSettings.getHazelcastVersionSpec());
        }
        return hazelcastVersionSpecs;
    }

    void setAgentWorkerMode(AgentWorkerMode agentWorkerMode) {
        this.agentWorkerMode = agentWorkerMode;
    }

    AgentWorkerMode getAgentWorkerMode() {
        return agentWorkerMode;
    }

    void addWorker(WorkerType type, WorkerParameters parameters, WorkerConfiguration workerConfiguration) {
        workerProcessSettingsList.add(
                new WorkerProcessSettings(
                        currentWorkerIndex.incrementAndGet(),
                        type,
                        parameters,
                        workerConfiguration.getHzVersion(),
                        workerConfiguration.getJvmOptions(),
                        workerConfiguration.getHzConfig()));
    }

    void addWorker(WorkerType type, WorkerParameters parameters) {
        workerProcessSettingsList.add(
                new WorkerProcessSettings(
                        currentWorkerIndex.incrementAndGet(),
                        type,
                        parameters));
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
