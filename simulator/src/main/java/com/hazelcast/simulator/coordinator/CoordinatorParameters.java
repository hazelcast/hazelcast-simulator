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

import static java.lang.Boolean.parseBoolean;

/**
 * Parameters for Simulator Coordinator.
 */
class CoordinatorParameters {

    private final SimulatorProperties simulatorProperties;
    private final String workerClassPath;

    private final boolean uploadHazelcastJARs;
    private final boolean enterpriseEnabled;
    private final boolean verifyEnabled;
    private final boolean parallel;
    private final boolean refreshJvm;
    private final boolean passiveMembers;

    private final TestPhase lastTestPhaseToSync;

    public CoordinatorParameters(SimulatorProperties properties, String workerClassPath, boolean uploadHazelcastJARs,
                                 boolean enterpriseEnabled, boolean verifyEnabled, boolean parallel, boolean refreshJvm,
                                 TestPhase lastTestPhaseToSync) {
        this.simulatorProperties = properties;
        this.workerClassPath = workerClassPath;

        this.uploadHazelcastJARs = uploadHazelcastJARs;
        this.enterpriseEnabled = enterpriseEnabled;
        this.verifyEnabled = verifyEnabled;
        this.parallel = parallel;
        this.refreshJvm = refreshJvm;
        this.passiveMembers = parseBoolean(properties.get("PASSIVE_MEMBERS", "true"));

        this.lastTestPhaseToSync = lastTestPhaseToSync;
    }

    public SimulatorProperties getSimulatorProperties() {
        return simulatorProperties;
    }

    public String getWorkerClassPath() {
        return workerClassPath;
    }

    public boolean isUploadHazelcastJARs() {
        return uploadHazelcastJARs;
    }

    public boolean isEnterpriseEnabled() {
        return enterpriseEnabled;
    }

    public boolean isVerifyEnabled() {
        return verifyEnabled;
    }

    public boolean isParallel() {
        return parallel;
    }

    public boolean isRefreshJvm() {
        return refreshJvm;
    }

    public boolean isPassiveMembers() {
        return passiveMembers;
    }

    public TestPhase getLastTestPhaseToSync() {
        return lastTestPhaseToSync;
    }
}
