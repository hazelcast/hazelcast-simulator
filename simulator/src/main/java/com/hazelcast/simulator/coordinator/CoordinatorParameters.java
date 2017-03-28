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
import com.hazelcast.simulator.common.TestPhase;

import java.text.SimpleDateFormat;
import java.util.Date;

import static com.hazelcast.simulator.utils.Preconditions.checkNotNull;

/**
 * Parameters for Simulator Coordinator.
 */
public class CoordinatorParameters {

    private String sessionId = new SimpleDateFormat("yyyy-MM-dd__HH_mm_ss").format(new Date());
    private TestPhase lastTestPhaseToSync = TestPhase.getLastTestPhase();

    private SimulatorProperties simulatorProperties;
    private boolean skipDownload;
    private boolean skipShutdownHook;
    private int workerVmStartupDelayMs;
    private int performanceMonitorIntervalSeconds;

    public String getSessionId() {
        return sessionId;
    }

    public CoordinatorParameters setSessionId(String sessionId) {
        this.sessionId = checkNotNull(sessionId, "sessionId can't be null");
        return this;
    }

    public TestPhase getLastTestPhaseToSync() {
        return lastTestPhaseToSync;
    }

    public CoordinatorParameters setLastTestPhaseToSync(TestPhase lastTestPhaseToSync) {
        this.lastTestPhaseToSync = lastTestPhaseToSync;
        return this;
    }

    public SimulatorProperties getSimulatorProperties() {
        return simulatorProperties;
    }

    public CoordinatorParameters setSimulatorProperties(SimulatorProperties simulatorProperties) {
        this.simulatorProperties = simulatorProperties;
        return this;
    }

    public boolean skipDownload() {
        return skipDownload;
    }

    public CoordinatorParameters setSkipDownload(boolean skipDownload) {
        this.skipDownload = skipDownload;
        return this;
    }

    public boolean skipShutdownHook() {
        return skipShutdownHook;
    }

    public CoordinatorParameters setSkipShutdownHook(boolean skipShutdownHook) {
        this.skipShutdownHook = skipShutdownHook;
        return this;
    }

    public int getWorkerVmStartupDelayMs() {
        return workerVmStartupDelayMs;
    }

    public CoordinatorParameters setWorkerVmStartupDelayMs(int workerVmStartupDelayMs) {
        this.workerVmStartupDelayMs = workerVmStartupDelayMs;
        return this;
    }

    public int getPerformanceMonitorIntervalSeconds() {
        return performanceMonitorIntervalSeconds;
    }

    public CoordinatorParameters setPerformanceMonitorIntervalSeconds(int performanceMonitorIntervalSeconds) {
        this.performanceMonitorIntervalSeconds = performanceMonitorIntervalSeconds;
        return this;
    }
}
