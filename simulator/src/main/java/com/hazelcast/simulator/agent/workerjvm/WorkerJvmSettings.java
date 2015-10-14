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
package com.hazelcast.simulator.agent.workerjvm;

import com.hazelcast.simulator.common.JavaProfiler;
import com.hazelcast.simulator.coordinator.WorkerParameters;
import com.hazelcast.simulator.worker.WorkerType;

/**
 * Settings for a (single) Simulator Worker JVM.
 */
public class WorkerJvmSettings {

    private final int workerIndex;
    private final String workerType;

    private final String jvmOptions;
    private final String hazelcastConfig;
    private final String log4jConfig;

    private final boolean autoCreateHzInstance;
    private final int workerStartupTimeout;
    private final int workerPerformanceMonitorIntervalSeconds;

    private final String profiler;
    private final String profilerSettings;
    private final String numaCtl;

    public WorkerJvmSettings(int workerIndex, WorkerType workerType, WorkerParameters workerParameters) {
        this.workerIndex = workerIndex;
        this.workerType = workerType.name();

        switch (workerType) {
            case MEMBER:
                this.jvmOptions = workerParameters.getMemberJvmOptions();
                this.hazelcastConfig = workerParameters.getMemberHzConfig();
                break;
            default:
                this.jvmOptions = workerParameters.getClientJvmOptions();
                this.hazelcastConfig = workerParameters.getClientHzConfig();
        }
        this.log4jConfig = workerParameters.getLog4jConfig();

        this.autoCreateHzInstance = workerParameters.isAutoCreateHzInstance();
        this.workerStartupTimeout = workerParameters.getWorkerStartupTimeout();
        this.workerPerformanceMonitorIntervalSeconds = initWorkerPerformanceMonitorIntervalSeconds(workerParameters);

        this.profiler = workerParameters.getProfiler().name();
        this.profilerSettings = workerParameters.getProfilerSettings();
        this.numaCtl = workerParameters.getNumaCtl();
    }

    private int initWorkerPerformanceMonitorIntervalSeconds(WorkerParameters workerParameters) {
        if (workerParameters.isMonitorPerformance()) {
            return workerParameters.getWorkerPerformanceMonitorIntervalSeconds();
        }
        return -1;
    }

    public int getWorkerIndex() {
        return workerIndex;
    }

    public WorkerType getWorkerType() {
        return WorkerType.valueOf(workerType);
    }

    public String getJvmOptions() {
        return jvmOptions;
    }

    public String getHazelcastConfig() {
        return hazelcastConfig;
    }

    public String getLog4jConfig() {
        return log4jConfig;
    }

    public boolean isAutoCreateHzInstance() {
        return autoCreateHzInstance;
    }

    public int getWorkerStartupTimeout() {
        return workerStartupTimeout;
    }

    public int getWorkerPerformanceMonitorIntervalSeconds() {
        return workerPerformanceMonitorIntervalSeconds;
    }

    public JavaProfiler getProfiler() {
        return JavaProfiler.valueOf(profiler);
    }

    public String getProfilerSettings() {
        return profilerSettings;
    }

    public String getNumaCtl() {
        return numaCtl;
    }

    @Override
    public String toString() {
        return "WorkerJvmSettings{"
                + "workerIndex=" + workerIndex
                + ", type=" + workerType
                + ", jvmOptions='" + jvmOptions + '\''
                + ", hazelcastConfig='" + hazelcastConfig + '\''
                + ", log4jConfig='" + log4jConfig + '\''
                + ", autoCreateHzInstance=" + autoCreateHzInstance
                + ", workerStartupTimeout=" + workerStartupTimeout
                + ", workerPerformanceMonitorIntervalSeconds=" + workerPerformanceMonitorIntervalSeconds
                + ", profiler='" + profiler + '\''
                + ", profilerSettings='" + profilerSettings + '\''
                + ", numaCtl='" + numaCtl + '\''
                + '}';
    }
}
