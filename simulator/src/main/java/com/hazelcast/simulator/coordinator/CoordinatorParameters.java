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

import com.hazelcast.simulator.common.SimulatorProperties;
import com.hazelcast.simulator.protocol.registry.TargetType;
import com.hazelcast.simulator.test.TestPhase;

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

    private final TargetType targetType;
    private final int targetCount;

    private final TestPhase lastTestPhaseToSync;

    CoordinatorParameters(SimulatorProperties properties, String workerClassPath, boolean uploadHazelcastJARs,
                          boolean enterpriseEnabled, boolean verifyEnabled, boolean parallel, boolean refreshJvm,
                          TargetType targetType, int targetCount, TestPhase lastTestPhaseToSync) {
        this.simulatorProperties = properties;
        this.workerClassPath = workerClassPath;

        this.uploadHazelcastJARs = uploadHazelcastJARs;
        this.enterpriseEnabled = enterpriseEnabled;
        this.verifyEnabled = verifyEnabled;
        this.parallel = parallel;
        this.refreshJvm = refreshJvm;

        this.targetType = targetType;
        this.targetCount = targetCount;

        this.lastTestPhaseToSync = lastTestPhaseToSync;
    }

    SimulatorProperties getSimulatorProperties() {
        return simulatorProperties;
    }

    String getWorkerClassPath() {
        return workerClassPath;
    }

    boolean isUploadHazelcastJARs() {
        return uploadHazelcastJARs;
    }

    boolean isEnterpriseEnabled() {
        return enterpriseEnabled;
    }

    boolean isVerifyEnabled() {
        return verifyEnabled;
    }

    boolean isParallel() {
        return parallel;
    }

    boolean isRefreshJvm() {
        return refreshJvm;
    }

    TargetType getTargetType(boolean hasClientWorkers) {
        return targetType.resolvePreferClient(hasClientWorkers);
    }

    int getTargetCount() {
        return targetCount;
    }

    TestPhase getLastTestPhaseToSync() {
        return lastTestPhaseToSync;
    }
}
