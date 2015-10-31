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
package com.hazelcast.simulator.cluster;

import com.hazelcast.simulator.agent.workerjvm.WorkerJvmSettings;
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

    private final List<WorkerJvmSettings> workerJvmSettingsList = new ArrayList<WorkerJvmSettings>();
    private final AtomicInteger currentWorkerIndex = new AtomicInteger();

    private final AgentData agentData;

    private AgentWorkerMode agentWorkerMode;

    AgentWorkerLayout(AgentData agentData, AgentWorkerMode agentWorkerMode) {
        this.agentData = agentData;
        this.agentWorkerMode = agentWorkerMode;
    }

    public List<WorkerJvmSettings> getWorkerJvmSettings() {
        return workerJvmSettingsList;
    }

    public SimulatorAddress getSimulatorAddress() {
        return agentData.getAddress();
    }

    public String getPublicAddress() {
        return agentData.getPublicAddress();
    }

    public Set<String> getHazelcastVersionSpecs() {
        Set<String> hazelcastVersionSpecs = new HashSet<String>();
        for (WorkerJvmSettings workerJvmSettings : workerJvmSettingsList) {
            hazelcastVersionSpecs.add(workerJvmSettings.getHazelcastVersionSpec());
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
        workerJvmSettingsList.add(new WorkerJvmSettings(currentWorkerIndex.incrementAndGet(), type, parameters,
                workerConfiguration.getHzVersion(), workerConfiguration.getJvmOptions(),
                workerConfiguration.getHzConfig()));
    }

    void addWorker(WorkerType type, WorkerParameters parameters) {
        workerJvmSettingsList.add(new WorkerJvmSettings(currentWorkerIndex.incrementAndGet(), type, parameters));
    }

    int getCount(WorkerType type) {
        int count = 0;
        for (WorkerJvmSettings workerJvmSettings : workerJvmSettingsList) {
            if (workerJvmSettings.getWorkerType() == type) {
                count++;
            }
        }
        return count;
    }
}
