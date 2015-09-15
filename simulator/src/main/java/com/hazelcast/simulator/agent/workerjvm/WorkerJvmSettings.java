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
import com.hazelcast.simulator.coordinator.CoordinatorParameters;
import com.hazelcast.simulator.worker.WorkerType;

import java.io.Serializable;

/**
 * Settings for a (single) Simulator Worker JVM.
 */
public class WorkerJvmSettings implements Serializable {

    private final int workerIndex;
    private final String workerType;

    private final String jvmOptions;
    private final String hazelcastConfig;
    private final String log4jConfig;

    private final boolean autoCreateHzInstance;
    private final int workerStartupTimeout;

    private final String profiler;
    private final String profilerSettings;
    private final String numaCtl;

    public WorkerJvmSettings(int workerIndex, WorkerType workerType, CoordinatorParameters parameters) {
        this.workerIndex = workerIndex;
        this.workerType = workerType.name();

        switch (workerType) {
            case MEMBER:
                this.jvmOptions = parameters.getMemberJvmOptions();
                this.hazelcastConfig = parameters.getMemberHzConfig();
                break;
            default:
                this.jvmOptions = parameters.getClientJvmOptions();
                this.hazelcastConfig = parameters.getClientHzConfig();
        }
        this.log4jConfig = parameters.getLog4jConfig();

        this.autoCreateHzInstance = parameters.isAutoCreateHzInstance();
        this.workerStartupTimeout = parameters.getWorkerStartupTimeout();

        this.profiler = parameters.getProfiler().name();
        this.profilerSettings = parameters.getProfilerSettings();
        this.numaCtl = parameters.getNumaCtl();
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
                + ", profiler='" + profiler + '\''
                + ", profilerSettings='" + profilerSettings + '\''
                + ", numaCtl='" + numaCtl + '\''
                + '}';
    }
}
