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
package com.hazelcast.simulator.protocol.operation;

import com.hazelcast.simulator.common.WorkerType;

import java.util.List;

public class RcWorkerStartOperation implements SimulatorOperation {
    private int count = 1;
    private String versionSpec;
    private String vmOptions = "";
    private String workerType = WorkerType.MEMBER.name();
    private String hzConfig;
    private List<String> agentAddresses;

    public RcWorkerStartOperation() {

    }

    public RcWorkerStartOperation(int count,
                                  String versionSpec,
                                  String vmOptions,
                                  String workerType,
                                  String hzConfig,
                                  List<String> agentAddresses) {
        this.count = count;
        this.versionSpec = versionSpec;
        this.vmOptions = vmOptions;
        this.workerType = workerType;
        this.hzConfig = hzConfig;
        this.agentAddresses = agentAddresses;
    }

    public RcWorkerStartOperation setCount(int count) {
        this.count = count;
        return this;
    }

    public RcWorkerStartOperation setVersionSpec(String versionSpec) {
        this.versionSpec = versionSpec;
        return this;
    }

    public RcWorkerStartOperation setVmOptions(String vmOptions) {
        this.vmOptions = vmOptions;
        return this;
    }

    public RcWorkerStartOperation setWorkerType(String workerType) {
        this.workerType = workerType;
        return this;
    }

    public RcWorkerStartOperation setHzConfig(String hzConfig) {
        this.hzConfig = hzConfig;
        return this;
    }

    public RcWorkerStartOperation setAgentAddresses(List<String> agentAddresses) {
        this.agentAddresses = agentAddresses;
        return this;
    }

    public int getCount() {
        return count;
    }

    public String getVersionSpec() {
        return versionSpec;
    }

    public String getVmOptions() {
        return vmOptions;
    }

    public String getWorkerType() {
        return workerType;
    }

    public String getHzConfig() {
        return hzConfig;
    }

    public List<String> getAgentAddress() {
        return agentAddresses;
    }
}
