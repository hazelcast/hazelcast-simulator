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

import com.hazelcast.simulator.common.SimulatorProperties;
import com.hazelcast.simulator.test.TestPhase;
import com.hazelcast.simulator.utils.CommandLineExitException;

import java.io.File;

import static java.lang.Boolean.parseBoolean;

/**
 * Parameters for Simulator Coordinator.
 */
class CoordinatorParameters {

    private final SimulatorProperties simulatorProperties;
    private final File agentsFile;
    private final String workerClassPath;

    private final boolean monitorPerformance;
    private final boolean verifyEnabled;
    private final boolean parallel;
    private final TestPhase lastTestPhaseToSync;

    private final boolean refreshJvm;
    private final boolean passiveMembers;

    private final int dedicatedMemberMachineCount;
    private final int clientWorkerCount;

    private int memberWorkerCount;

    @SuppressWarnings("checkstyle:parameternumber")
    public CoordinatorParameters(SimulatorProperties properties, File agentsFile, String workerClassPath,
                                 boolean monitorPerformance, boolean verifyEnabled, boolean parallel,
                                 TestPhase lastTestPhaseToSync, boolean refreshJvm, int dedicatedMemberMachineCount,
                                 int memberWorkerCount, int clientWorkerCount) {
        if (dedicatedMemberMachineCount < 0) {
            throw new CommandLineExitException("--dedicatedMemberMachines can't be smaller than 0");
        }

        this.simulatorProperties = properties;
        this.agentsFile = agentsFile;
        this.workerClassPath = workerClassPath;
        this.monitorPerformance = monitorPerformance;
        this.verifyEnabled = verifyEnabled;
        this.parallel = parallel;
        this.lastTestPhaseToSync = lastTestPhaseToSync;

        this.refreshJvm = refreshJvm;
        this.passiveMembers = parseBoolean(simulatorProperties.get("PASSIVE_MEMBERS", "true"));

        this.dedicatedMemberMachineCount = dedicatedMemberMachineCount;
        this.clientWorkerCount = clientWorkerCount;

        this.memberWorkerCount = memberWorkerCount;
    }

    public SimulatorProperties getSimulatorProperties() {
        return simulatorProperties;
    }

    public File getAgentsFile() {
        return agentsFile;
    }

    public String getWorkerClassPath() {
        return workerClassPath;
    }

    public boolean isMonitorPerformance() {
        return monitorPerformance;
    }

    public boolean isVerifyEnabled() {
        return verifyEnabled;
    }

    public boolean isParallel() {
        return parallel;
    }

    public TestPhase getLastTestPhaseToSync() {
        return lastTestPhaseToSync;
    }

    public boolean isRefreshJvm() {
        return refreshJvm;
    }

    public boolean isPassiveMembers() {
        return passiveMembers;
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

    public void setMemberWorkerCount(int memberWorkerCount) {
        this.memberWorkerCount = memberWorkerCount;
    }
}
