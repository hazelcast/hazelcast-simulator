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

import com.hazelcast.simulator.common.JavaProfiler;
import com.hazelcast.simulator.common.SimulatorProperties;

/**
 * Parameters for Simulator Worker.
 */
public class WorkerParameters {

    private final int workerStartupTimeout;
    private final boolean autoCreateHzInstance;

    private final String memberJvmOptions;
    private final String clientJvmOptions;

    private String memberHzConfig;
    private String clientHzConfig;
    private final String log4jConfig;

    private final JavaProfiler profiler;
    private final String profilerSettings;
    private final String numaCtl;

    public WorkerParameters(SimulatorProperties properties, int workerStartupTimeout, boolean autoCreateHzInstance,
                            String memberJvmOptions, String clientJvmOptions, String memberHzConfig, String clientHzConfig,
                            String log4jConfig) {
        this.workerStartupTimeout = workerStartupTimeout;
        this.autoCreateHzInstance = autoCreateHzInstance;

        this.memberJvmOptions = memberJvmOptions;
        this.clientJvmOptions = clientJvmOptions;

        this.memberHzConfig = memberHzConfig;
        this.clientHzConfig = clientHzConfig;
        this.log4jConfig = log4jConfig;

        this.profiler = initProfiler(properties);
        this.profilerSettings = initProfilerSettings(properties);
        this.numaCtl = properties.get("NUMA_CONTROL", "none");
    }

    private JavaProfiler initProfiler(SimulatorProperties properties) {
        String profilerName = properties.get("PROFILER");
        if (profilerName == null || profilerName.isEmpty()) {
            return JavaProfiler.NONE;
        }
        return JavaProfiler.valueOf(profilerName.toUpperCase());
    }

    private String initProfilerSettings(SimulatorProperties properties) {
        switch (profiler) {
            case YOURKIT:
            case FLIGHTRECORDER:
                return properties.get(profiler.name() + "_SETTINGS");
            case HPROF:
            case PERF:
            case VTUNE:
                return properties.get(profiler.name() + "_SETTINGS", "");
            default:
                return "";
        }
    }

    public int getWorkerStartupTimeout() {
        return workerStartupTimeout;
    }

    public boolean isAutoCreateHzInstance() {
        return autoCreateHzInstance;
    }

    public String getMemberJvmOptions() {
        return memberJvmOptions;
    }

    public String getClientJvmOptions() {
        return clientJvmOptions;
    }

    public String getMemberHzConfig() {
        return memberHzConfig;
    }

    public void setMemberHzConfig(String memberHzConfig) {
        this.memberHzConfig = memberHzConfig;
    }

    public String getClientHzConfig() {
        return clientHzConfig;
    }

    public void setClientHzConfig(String clientHzConfig) {
        this.clientHzConfig = clientHzConfig;
    }

    public String getLog4jConfig() {
        return log4jConfig;
    }

    public JavaProfiler getProfiler() {
        return profiler;
    }

    public String getProfilerSettings() {
        return profilerSettings;
    }

    public String getNumaCtl() {
        return numaCtl;
    }
}
