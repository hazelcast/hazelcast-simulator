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
import com.hazelcast.simulator.testcontainer.TestPhase;

/**
 * Parameters for Simulator Coordinator.
 */
class CoordinatorParameters {

    private final SimulatorProperties simulatorProperties;
    private final String workerClassPath;

    private final boolean skipDownload;
    private final boolean verifyEnabled;
    private final boolean parallel;

    private final TargetType targetType;
    private final int targetCount;

    private final TestPhase lastTestPhaseToSync;
    private final int workerVmStartupDelayMs;
    private final String afterCompletionFile;

    @SuppressWarnings("checkstyle:parameternumber")
    CoordinatorParameters(SimulatorProperties properties,
                          String workerClassPath,
                          boolean verifyEnabled,
                          boolean parallel,
                          TargetType targetType,
                          int targetCount,
                          TestPhase lastTestPhaseToSync,
                          int workerVmStartupDelayMs,
                          boolean skipDownload,
                          String afterCompletionFile) {
        this.simulatorProperties = properties;
        this.workerClassPath = workerClassPath;
        this.verifyEnabled = verifyEnabled;
        this.parallel = parallel;
        this.targetType = targetType;
        this.targetCount = targetCount;
        this.lastTestPhaseToSync = lastTestPhaseToSync;
        this.workerVmStartupDelayMs = workerVmStartupDelayMs;
        this.skipDownload = skipDownload;
        this.afterCompletionFile = afterCompletionFile;
    }

    public int getWorkerVmStartupDelayMs() {
        return workerVmStartupDelayMs;
    }

    SimulatorProperties getSimulatorProperties() {
        return simulatorProperties;
    }

    String getWorkerClassPath() {
        return workerClassPath;
    }

    boolean isVerifyEnabled() {
        return verifyEnabled;
    }

    boolean isParallel() {
        return parallel;
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

    public boolean skipDownload() {
        return skipDownload;
    }

    public String getAfterCompletionFile() {
        return afterCompletionFile;
    }
}
