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
import com.hazelcast.simulator.utils.Preconditions;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Parameters for Simulator Coordinator.
 */
public class CoordinatorParameters {

    private SimulatorProperties simulatorProperties;
    private String workerClassPath;
    private boolean skipDownload;
    private TestPhase lastTestPhaseToSync = TestPhase.getLastTestPhase();
    private int workerVmStartupDelayMs;
    private String afterCompletionFile;
    private String sessionId = new SimpleDateFormat("yyyy-MM-dd__HH_mm_ss").format(new Date());
    private int performanceMonitorIntervalSeconds;
    private String licenseKey;

    public String getLicenseKey() {
        return licenseKey;
    }

    public CoordinatorParameters setLicenseKey(String licenseKey) {
        this.licenseKey = licenseKey;
        return this;
    }

    public int getPerformanceMonitorIntervalSeconds() {
        return performanceMonitorIntervalSeconds;
    }

    public CoordinatorParameters setPerformanceMonitorIntervalSeconds(int performanceMonitorIntervalSeconds) {
        this.performanceMonitorIntervalSeconds = performanceMonitorIntervalSeconds;
        return this;
    }

    public String getSessionId() {
        return sessionId;
    }

    public CoordinatorParameters setSessionId(String sessionId) {
        this.sessionId = Preconditions.checkNotNull(sessionId, "sessionId can't be null");
        return this;
    }

    public int getWorkerVmStartupDelayMs() {
        return workerVmStartupDelayMs;
    }

    public CoordinatorParameters setWorkerVmStartupDelayMs(int workerVmStartupDelayMs) {
        this.workerVmStartupDelayMs = workerVmStartupDelayMs;
        return this;
    }

    public SimulatorProperties getSimulatorProperties() {
        return simulatorProperties;
    }

    public CoordinatorParameters setSimulatorProperties(SimulatorProperties simulatorProperties) {
        this.simulatorProperties = simulatorProperties;
        return this;
    }

    public String getWorkerClassPath() {
        return workerClassPath;
    }

    public CoordinatorParameters setWorkerClassPath(String workerClassPath) {
        this.workerClassPath = workerClassPath;
        return this;
    }

    public TestPhase getLastTestPhaseToSync() {
        return lastTestPhaseToSync;
    }

    public CoordinatorParameters setLastTestPhaseToSync(TestPhase lastTestPhaseToSync) {
        this.lastTestPhaseToSync = lastTestPhaseToSync;
        return this;
    }

    public boolean skipDownload() {
        return skipDownload;
    }

    public String getAfterCompletionFile() {
        return afterCompletionFile;
    }

    public CoordinatorParameters setAfterCompletionFile(String afterCompletionFile) {
        this.afterCompletionFile = afterCompletionFile;
        return this;
    }

    public CoordinatorParameters setSkipDownload(boolean skipDownload) {
        this.skipDownload = skipDownload;
        return this;
    }
}
