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
import com.hazelcast.simulator.testcontainer.TestPhase;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Parameters for Simulator Coordinator.
 */
class CoordinatorParameters {

    private final SimulatorProperties simulatorProperties;
    private final String workerClassPath;
    private final boolean skipDownload;
    private final TestPhase lastTestPhaseToSync;
    private final int workerVmStartupDelayMs;
    private final String afterCompletionFile;
    private final String sessionId;

    @SuppressWarnings("checkstyle:parameternumber")
    CoordinatorParameters(String sessionId,
                          SimulatorProperties properties,
                          String workerClassPath,
                          TestPhase lastTestPhaseToSync,
                          int workerVmStartupDelayMs,
                          boolean skipDownload,
                          String afterCompletionFile) {
        this.sessionId = sessionId == null ? createSessionId() : sessionId;
        this.simulatorProperties = properties;
        this.workerClassPath = workerClassPath;
        this.lastTestPhaseToSync = lastTestPhaseToSync;
        this.workerVmStartupDelayMs = workerVmStartupDelayMs;
        this.skipDownload = skipDownload;
        this.afterCompletionFile = afterCompletionFile;
    }

    String getSessionId() {
        return sessionId;
    }

    int getWorkerVmStartupDelayMs() {
        return workerVmStartupDelayMs;
    }

    SimulatorProperties getSimulatorProperties() {
        return simulatorProperties;
    }

    String getWorkerClassPath() {
        return workerClassPath;
    }

    TestPhase getLastTestPhaseToSync() {
        return lastTestPhaseToSync;
    }

    boolean skipDownload() {
        return skipDownload;
    }

    String getAfterCompletionFile() {
        return afterCompletionFile;
    }

    private static String createSessionId() {
        return new SimpleDateFormat("yyyy-MM-dd__HH_mm_ss").format(new Date());
    }
}
