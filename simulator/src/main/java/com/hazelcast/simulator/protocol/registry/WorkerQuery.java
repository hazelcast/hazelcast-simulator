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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.hazelcast.simulator.utils.TagUtils.matches;
import static java.util.Collections.shuffle;

public class WorkerQuery {

    private String versionSpec;
    private List<String> workerAddresses;
    private List<String> agentAddresses;
    private Integer maxCount;
    private WorkerType workerType;
    private boolean random;
    private TargetType targetType = TargetType.ALL;
    private Map<String, String> workerTags;

    public Map<String, String> getWorkerTags() {
        return workerTags;
    }

    public WorkerQuery setWorkerTags(Map<String, String> workerTags) {
        this.workerTags = workerTags;
        return this;
    }

    public TargetType getTargetType() {
        return targetType;
    }

    public WorkerQuery setTargetType(TargetType targetType) {
        this.targetType = targetType;
        return this;
    }

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

    public List<String> getWorkerAddresses() {
        return workerAddresses;
    }

    public WorkerQuery setWorkerAddresses(List<String> workerAddresses) {
        this.workerAddresses = workerAddresses;
        return this;
    }

    public List<String> getAgentAddresses() {
        return agentAddresses;
    }

    public WorkerQuery setAgentAddresses(List<String> agentAddresses) {
        this.agentAddresses = agentAddresses;
        return this;
    }

    public Integer getMaxCount() {
        return maxCount;
    }

    public WorkerQuery setMaxCount(Integer maxCount) {
        this.maxCount = maxCount;
        return this;
    }

    public WorkerType getWorkerType() {
        return workerType;
    }

    public WorkerQuery setWorkerType(String workerType) {
        this.workerType = workerType == null ? null : new WorkerType(workerType);
        return this;
    }

    public List<WorkerData> execute(List<WorkerData> input) {
        switch (targetType) {
            case ALL:
                return select(input, null);
            case MEMBER:
                return select(input, true);
            case CLIENT:
                return select(input, false);
            case PREFER_CLIENT:
                List<WorkerData> result = select(input, false);
                return !result.isEmpty() ? result : select(input, true);
            default:
                throw new IllegalStateException("Unrecognized targetType: " + targetType);
        }
    }

    private List<WorkerData> select(List<WorkerData> input, Boolean isMember) {
        if (random) {
            input = randomize(input);
        }

        List<WorkerData> result = new ArrayList<WorkerData>(input.size());
        for (WorkerData worker : input) {
            if (hasConflict(worker, isMember)) {
                continue;
            }

            if (maxCount != null && result.size() == maxCount) {
                break;
            }

            result.add(worker);
        }
        return result;
    }

    private List<WorkerData> randomize(List<WorkerData> workers) {
        List<WorkerData> result = new ArrayList<WorkerData>(workers);
        shuffle(workers);
        return result;
    }

    @SuppressWarnings("checkstyle:booleanexpressioncomplexity")
    private boolean hasConflict(WorkerData worker, Boolean isMember) {
        return hasIsMemberConflict(worker, isMember)
                || hasVersionSpecConflict(worker)
                || hasWorkerAddressConflict(worker)
                || hasWorkerTagConflict(worker)
                || hasAgentAddressConflict(worker)
                || hasWorkerTypeConflict(worker);
    }

    private boolean hasWorkerTypeConflict(WorkerData worker) {
        WorkerProcessSettings workerProcessSettings = worker.getSettings();

        if (workerType != null) {
            if (!workerProcessSettings.getWorkerType().equals(workerType)) {
                return true;
            }
        }
        return false;
    }

    private boolean hasAgentAddressConflict(WorkerData worker) {
        if (agentAddresses != null) {
            boolean found = false;
            for (String agentAddress : agentAddresses) {
                if (worker.getAddress().getParent().toString().equals(agentAddress)) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                return true;
            }
        }
        return false;
    }

    private boolean hasWorkerAddressConflict(WorkerData worker) {
        if (workerAddresses != null) {
            boolean found = false;
            for (String address : workerAddresses) {
                if (worker.getAddress().toString().equals(address)) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                return true;
            }
        }
        return false;
    }

    private boolean hasWorkerTagConflict(WorkerData worker) {
        if (workerTags != null) {
            return !matches(workerTags, worker.getTags());
        }
        return false;
    }

    private boolean hasVersionSpecConflict(WorkerData worker) {
        WorkerProcessSettings workerProcessSettings = worker.getSettings();

        if (versionSpec != null) {
            if (!workerProcessSettings.getVersionSpec().equals(versionSpec)) {
                return true;
            }
        }
        return false;
    }

    private boolean hasIsMemberConflict(WorkerData worker, Boolean isMember) {
        if (isMember != null) {
            if (worker.isMemberWorker() != isMember) {
                return true;
            }
        }
        return false;
    }

    @Override
    public String toString() {
        return "WorkerQuery{"
                + "versionSpec='" + versionSpec + '\''
                + ", maxCount=" + maxCount
                + ", workerType=" + workerType
                + ", random=" + random
                + ", targetType=" + targetType
                + ", workerAddresses=" + workerAddresses
                + ", agentAddresses=" + agentAddresses
                + '}';
    }
}
