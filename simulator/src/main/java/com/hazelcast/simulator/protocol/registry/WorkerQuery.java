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
package com.hazelcast.simulator.protocol.registry;

import com.hazelcast.simulator.agent.workerprocess.WorkerProcessSettings;
import com.hazelcast.simulator.common.WorkerType;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class WorkerQuery {

    private String versionSpec;
    private SimulatorAddress workerAddress;
    private SimulatorAddress agentAddress;
    private int maxCount;
    private WorkerType workerType;
    private boolean random;

    public boolean isRandom() {
        return random;
    }

    public WorkerQuery setRandom(boolean random) {
        this.random = random;
        return this;
    }

    public String getVersionSpec() {
        return versionSpec;
    }

    public WorkerQuery setVersionSpec(String versionSpec) {
        this.versionSpec = versionSpec;
        return this;
    }

    public SimulatorAddress getWorkerAddress() {
        return workerAddress;
    }

    public WorkerQuery setWorkerAddress(String workerAddress) {
        this.workerAddress = workerAddress == null ? null : SimulatorAddress.fromString(workerAddress);
        return this;
    }

    public SimulatorAddress getAgentAddress() {
        return agentAddress;
    }

    public WorkerQuery setAgentAddress(String agentAddress) {
        this.agentAddress = agentAddress == null ? null : SimulatorAddress.fromString(agentAddress);
        return this;
    }

    public int getMaxCount() {
        return maxCount;
    }

    public WorkerQuery setMaxCount(int count) {
        this.maxCount = count;
        return this;
    }

    public WorkerType getWorkerType() {
        return workerType;
    }

    public WorkerQuery setWorkerType(String workerType) {
        this.workerType = workerType == null ? null : new WorkerType(workerType);
        return this;
    }

    public List<WorkerData> execute(List<WorkerData> workers) {
        if (random) {
            workers = randomize(workers);
        }


        List<WorkerData> result = new ArrayList<WorkerData>(workers.size());
        for (WorkerData worker : workers) {
            if (!isVictim(worker)) {
                continue;
            }

            if (result.size() == maxCount) {
                break;
            }

            result.add(worker);
        }
        return result;
    }

    private List<WorkerData> randomize(List<WorkerData> workers) {
        List<WorkerData> result = new ArrayList<WorkerData>(workers);
        Collections.shuffle(workers);
        return result;
    }

    @SuppressWarnings("checkstyle:npathcomplexity")
    private boolean isVictim(WorkerData workerData) {
        WorkerProcessSettings workerProcessSettings = workerData.getSettings();

        if (versionSpec != null) {
            if (!workerProcessSettings.getVersionSpec().equals(versionSpec)) {
                return false;
            }
        }

        if (workerAddress != null) {
            if (!workerData.getAddress().equals(workerAddress)) {
                return false;
            }
        }

        if (agentAddress != null) {
            if (!workerData.getAddress().getParent().equals(agentAddress)) {
                return false;
            }
        }

        if (!workerProcessSettings.getWorkerType().equals(workerType)) {
            return false;
        }

        return true;
    }
}
