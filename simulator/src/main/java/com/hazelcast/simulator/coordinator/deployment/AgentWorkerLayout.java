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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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

    public List<WorkerProcessSettings> getWorkerProcessSettings() {
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

    public Set<String> getVersionSpecs() {
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

    void addWorker(WorkerType type, WorkerParameters parameters) {
        Map<String, String> environment = new HashMap<String, String>();
        environment.put("JVM_OPTIONS", type.isMember() ? parameters.getMemberJvmOptions() : parameters.getClientJvmOptions());
        environment.put("HAZELCAST_CONFIG", type.isMember() ? parameters.getMemberHzConfig() : parameters.getClientHzConfig());
        environment.put("LOG4j_CONFIG", parameters.getLog4jConfig());
        environment.put("AUTOCREATE_HAZELCAST_INSTANCE", Boolean.toString(parameters.isAutoCreateHzInstance()));
        environment.put("WORKER_PERFORMANCE_MONITOR_INTERVAL_SECONDS",
                Integer.toString(parameters.getWorkerPerformanceMonitorIntervalSeconds()));

        WorkerProcessSettings settings = new WorkerProcessSettings(
                currentWorkerIndex.incrementAndGet(),
                type,
                parameters.getVersionSpec(),
                parameters.getWorkerScript(),
                parameters.getWorkerStartupTimeout(),
                environment);
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
