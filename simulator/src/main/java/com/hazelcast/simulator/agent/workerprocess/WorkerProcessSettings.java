/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.simulator.common.WorkerType;

import java.util.Map;

/**
 * Settings for a (single) Simulator Worker Process.
 */
public class WorkerProcessSettings {

    private final int workerIndex;
    private final String workerType;
    private final String versionSpec;
    private final int workerStartupTimeout;
    private final String workerScript;
    private final Map<String, String> environment;

    public WorkerProcessSettings(int workerIndex,
                                 WorkerType workerType,
                                 String versionSpec,
                                 String workerScript,
                                 int workerStartupTimeout,
                                 Map<String, String> environment) {
        this.workerIndex = workerIndex;
        this.workerType = workerType.name();
        this.versionSpec = versionSpec;
        this.environment = environment;
        this.workerStartupTimeout = workerStartupTimeout;
        this.workerScript = workerScript;
    }

    public int getWorkerIndex() {
        return workerIndex;
    }

    public Map<String, String> getEnvironment() {
        return environment;
    }

    public WorkerType getWorkerType() {
        return new WorkerType(workerType);
    }

    public String getVersionSpec() {
        return versionSpec;
    }

    public int getWorkerStartupTimeout() {
        return workerStartupTimeout;
    }

    public String getWorkerScript() {
        return workerScript;
    }

    @Override
    public String toString() {
        return "WorkerProcessSettings{"
                + "workerIndex=" + workerIndex
                + ", versionSpec=" + versionSpec
                + ", type=" + workerType
                + ", workerStartupTimeout=" + workerStartupTimeout
                + ", workerScript='" + workerScript + '\''
                + ", environment=" + environment
                + '}';
    }
}
