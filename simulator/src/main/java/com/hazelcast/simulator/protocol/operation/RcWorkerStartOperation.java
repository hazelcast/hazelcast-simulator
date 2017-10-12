/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.simulator.protocol.operation;

import com.hazelcast.simulator.common.WorkerType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RcWorkerStartOperation implements SimulatorOperation {
    private int count = 1;
    private String versionSpec;
    private String vmOptions = "";
    private String workerType = WorkerType.MEMBER.name();
    private String hzConfig;
    private List<String> agentAddresses;
    private Map<String, String> tags = new HashMap<String, String>();
    private Map<String, String> agentTags;

    public Map<String, String> getAgentTags() {
        return agentTags;
    }

    public RcWorkerStartOperation setAgentTags(Map<String, String> agentTags) {
        this.agentTags = agentTags;
        return this;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    public RcWorkerStartOperation setTags(Map<String, String> environment) {
        this.tags = environment;
        return this;
    }

    public String getWorkerType() {
        return workerType;
    }

    public RcWorkerStartOperation setWorkerType(String workerType) {
        this.workerType = workerType;
        return this;
    }

    public String getHzConfig() {
        return hzConfig;
    }

    public RcWorkerStartOperation setHzConfig(String hzConfig) {
        this.hzConfig = hzConfig;
        return this;
    }

    public List<String> getAgentAddresses() {
        return agentAddresses;
    }

    public RcWorkerStartOperation setAgentAddresses(List<String> agentAddresses) {
        this.agentAddresses = agentAddresses;
        return this;
    }

    public RcWorkerStartOperation setCount(int count) {
        this.count = count;
        return this;
    }

    public int getCount() {
        return count;
    }

    public String getVersionSpec() {
        return versionSpec;
    }

    public RcWorkerStartOperation setVersionSpec(String versionSpec) {
        this.versionSpec = versionSpec;
        return this;
    }

    public String getVmOptions() {
        return vmOptions;
    }

    public RcWorkerStartOperation setVmOptions(String vmOptions) {
        this.vmOptions = vmOptions;
        return this;
    }
}
