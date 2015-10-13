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

import java.io.File;

import static java.lang.Boolean.parseBoolean;

/**
 * Parameters for Simulator Coordinator.
 */
class CoordinatorParameters {

    private final SimulatorProperties simulatorProperties;
    private final File agentsFile;
    private final String workerClassPath;

    private final boolean verifyEnabled;
    private final boolean parallel;
    private final TestPhase lastTestPhaseToSync;

    private final boolean refreshJvm;
    private final boolean passiveMembers;

    public CoordinatorParameters(SimulatorProperties properties, File agentsFile, String workerClassPath,
                                 boolean verifyEnabled, boolean parallel,
                                 TestPhase lastTestPhaseToSync, boolean refreshJvm) {
        this.simulatorProperties = properties;
        this.agentsFile = agentsFile;
        this.workerClassPath = workerClassPath;
        this.verifyEnabled = verifyEnabled;
        this.parallel = parallel;
        this.lastTestPhaseToSync = lastTestPhaseToSync;

        this.refreshJvm = refreshJvm;
        this.passiveMembers = parseBoolean(properties.get("PASSIVE_MEMBERS", "true"));
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
}
