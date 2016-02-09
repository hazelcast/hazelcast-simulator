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
package com.hazelcast.simulator.coordinator;

import com.hazelcast.simulator.cluster.WorkerConfigurationConverter;

/**
 * Parameters for the layout of a Simulator cluster.
 */
public class ClusterLayoutParameters {

    private final String clusterConfiguration;
    private final WorkerConfigurationConverter workerConfigurationConverter;

    private final int memberWorkerCount;
    private final int clientWorkerCount;
    private final int dedicatedMemberMachineCount;

    public ClusterLayoutParameters(String clusterConfiguration, WorkerConfigurationConverter workerConfigurationConverter,
                                   int memberWorkerCount, int clientWorkerCount, int dedicatedMemberMachineCount,
                                   int agentCount) {
        this.clusterConfiguration = clusterConfiguration;
        this.workerConfigurationConverter = workerConfigurationConverter;
        this.memberWorkerCount = (memberWorkerCount == -1) ? agentCount : memberWorkerCount;
        this.clientWorkerCount = clientWorkerCount;
        this.dedicatedMemberMachineCount = dedicatedMemberMachineCount;
    }

    public String getClusterConfiguration() {
        return clusterConfiguration;
    }

    public WorkerConfigurationConverter getWorkerConfigurationConverter() {
        return workerConfigurationConverter;
    }

    public int getMemberWorkerCount() {
        return memberWorkerCount;
    }

    public int getClientWorkerCount() {
        return clientWorkerCount;
    }

    public int getDedicatedMemberMachineCount() {
        return dedicatedMemberMachineCount;
    }
}
