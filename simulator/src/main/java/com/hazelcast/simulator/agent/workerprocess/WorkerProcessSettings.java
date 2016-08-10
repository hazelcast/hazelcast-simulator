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
package com.hazelcast.simulator.agent.workerprocess;

import com.hazelcast.simulator.coordinator.WorkerParameters;
import com.hazelcast.simulator.worker.WorkerType;

import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.simulator.worker.WorkerType.MEMBER;

/**
 * Settings for a (single) Simulator Worker Process.
 */
public class WorkerProcessSettings {

    private final int workerIndex;
    private final String workerType;
    private final String versionSpec;

    private final String jvmOptions;
    private final String hazelcastConfig;
    private final String log4jConfig;

    private final boolean autoCreateHzInstance;
    private final int workerStartupTimeout;
    private final int performanceMonitorIntervalSeconds;

    private final String workerScript;

    public WorkerProcessSettings(int workerIndex, WorkerType workerType, WorkerParameters workerParameters) {
        this(workerIndex, workerType, workerParameters, workerParameters.getVersionSpec(),
                (workerType == MEMBER) ? workerParameters.getMemberJvmOptions() : workerParameters.getClientJvmOptions(),
                (workerType == MEMBER) ? workerParameters.getMemberHzConfig() : workerParameters.getClientHzConfig());
    }

    public WorkerProcessSettings(int workerIndex, WorkerType workerType, WorkerParameters workerParameters,
                                 String versionSpec, String jvmOptions, String hazelcastConfig) {
        this.workerIndex = workerIndex;
        this.workerType = workerType.name();
        this.versionSpec = versionSpec;

        this.jvmOptions = jvmOptions;
        this.hazelcastConfig = hazelcastConfig;
        this.log4jConfig = workerParameters.getLog4jConfig();

        this.autoCreateHzInstance = workerParameters.isAutoCreateHzInstance();
        this.workerStartupTimeout = workerParameters.getWorkerStartupTimeout();
        this.performanceMonitorIntervalSeconds = initPerformanceMonitorIntervalSeconds(workerParameters);

        this.workerScript = workerParameters.getWorkerScript();
    }

    private int initPerformanceMonitorIntervalSeconds(WorkerParameters workerParameters) {
        if (workerParameters.isMonitorPerformance()) {
            return workerParameters.getWorkerPerformanceMonitorIntervalSeconds();
        }
        return -1;
    }

    public int getWorkerIndex() {
        return workerIndex;
    }

    public Map<String, String> getEnvironment() {
        Map<String, String> environment = new HashMap<String, String>();
        environment.put("HAZELCAST_CONFIG", hazelcastConfig);
        environment.put("LOG4j_CONFIG", log4jConfig);
        environment.put("AUTOCREATE_HAZELCAST_INSTANCE", Boolean.toString(autoCreateHzInstance));
        environment.put("JVM_OPTIONS", jvmOptions);
        return environment;
    }

    public WorkerType getWorkerType() {
        return WorkerType.valueOf(workerType);
    }

    public String getVersionSpec() {
        return versionSpec;
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

    public int getPerformanceMonitorIntervalSeconds() {
        return performanceMonitorIntervalSeconds;
    }

    public String getWorkerScript() {
        return workerScript;
    }

    @Override
    public String toString() {
        return "WorkerProcessSettings{"
                + "workerIndex=" + workerIndex
                + ", type=" + workerType
                + ", jvmOptions='" + jvmOptions + '\''
                + ", hazelcastConfig='" + hazelcastConfig + '\''
                + ", log4jConfig='" + log4jConfig + '\''
                + ", autoCreateHzInstance=" + autoCreateHzInstance
                + ", workerStartupTimeout=" + workerStartupTimeout
                + ", performanceMonitorIntervalSeconds=" + performanceMonitorIntervalSeconds
                + ", versionSpec='" + versionSpec + '\''
                + ", workerScript='" + workerScript + '\''
                + '}';
    }
}
