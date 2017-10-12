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
package com.hazelcast.simulator.coordinator;

import java.util.HashMap;
import java.util.Map;

/**
 * Parameters for Simulator Worker. The class itself should remain implementation independent; maybe it is a Java worker,
 * maybe it is a c++ Hazelcast client worker. The implementation specific variables, can be passed using the the environment,
 * see {@link #getEnvironment()}.
 */
public class WorkerParameters {

    private int workerStartupTimeout;
    private String versionSpec;
    private String workerScript;
    private Map<String, String> environment = new HashMap<String, String>();

    public int getWorkerStartupTimeout() {
        return workerStartupTimeout;
    }

    public WorkerParameters setWorkerStartupTimeout(int workerStartupTimeout) {
        this.workerStartupTimeout = workerStartupTimeout;
        return this;
    }

    public String getVersionSpec() {
        return versionSpec;
    }

    public WorkerParameters setVersionSpec(String versionSpec) {
        this.versionSpec = versionSpec;
        return this;
    }

    public String getWorkerScript() {
        return workerScript;
    }

    public WorkerParameters setWorkerScript(String workerScript) {
        this.workerScript = workerScript;
        return this;
    }

    public Map<String, String> getEnvironment() {
        return environment;
    }

    public WorkerParameters addEnvironment(String variable, String value) {
        environment.put(variable, value);
        return this;
    }

    public WorkerParameters addEnvironment(Map<String, String> env) {
        environment.putAll(env);
        return this;
    }

    public WorkerParameters setEnvironment(Map<String, String> environment) {
        this.environment = environment;
        return this;
    }
}
