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

public class RcWorkerStartOperation implements SimulatorOperation {
    private int count;
    private String versionSpec;
    private String vmOptions;
    private String workerType;
    private String hzConfig;
    private String agentAddress;

    public RcWorkerStartOperation(int count, String versionSpec, String vmOptions,
                                  String workerType, String hzConfig, String agentAddress) {
        this.count = count;
        this.versionSpec = versionSpec;
        this.vmOptions = vmOptions;
        this.workerType = workerType;
        this.hzConfig = hzConfig;
        this.agentAddress = agentAddress;
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

    public String getAgentAddress() {
        return agentAddress;
    }
}
