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
package com.hazelcast.simulator.coordinator;

/**
 * Parameters for the layout of a Simulator cluster.
 */
class ClusterLayoutParameters {

    private final int dedicatedMemberMachineCount;
    private final int clientWorkerCount;

    private int memberWorkerCount;

    public ClusterLayoutParameters(int dedicatedMemberMachineCount, int clientWorkerCount, int memberWorkerCount) {
        this.dedicatedMemberMachineCount = dedicatedMemberMachineCount;
        this.clientWorkerCount = clientWorkerCount;

        this.memberWorkerCount = memberWorkerCount;
    }

    public void initMemberWorkerCount(int agentCount) {
        if (memberWorkerCount == -1) {
            memberWorkerCount = agentCount;
        }
    }

    public int getDedicatedMemberMachineCount() {
        return dedicatedMemberMachineCount;
    }

    public int getClientWorkerCount() {
        return clientWorkerCount;
    }

    public int getMemberWorkerCount() {
        return memberWorkerCount;
    }
}
