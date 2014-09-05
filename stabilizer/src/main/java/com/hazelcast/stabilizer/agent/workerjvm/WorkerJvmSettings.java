/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.stabilizer.agent.workerjvm;

import java.io.Serializable;

public class WorkerJvmSettings implements Serializable {
    public String vmOptions;
    public String hzConfig;

    public int memberWorkerCount;
    public int clientWorkerCount;
    public int mixedWorkerCount;

    public int workerStartupTimeout;
    public boolean refreshJvm;
    public String javaVendor;
    public String javaVersion;
    public String clientHzConfig;
    public String profiler = "none";
    public String yourkitConfig;
    public String hprofSettings = "";
    public String clientVmOptions;

    public WorkerJvmSettings() {
    }

    public WorkerJvmSettings(WorkerJvmSettings settings) {
        this.vmOptions = settings.vmOptions;
        this.clientVmOptions = settings.clientVmOptions;
        this.hzConfig = settings.hzConfig;
        this.clientHzConfig = settings.clientHzConfig;
        this.memberWorkerCount = settings.memberWorkerCount;
        this.clientWorkerCount = settings.clientWorkerCount;
        this.mixedWorkerCount = settings.mixedWorkerCount;
        this.workerStartupTimeout = settings.workerStartupTimeout;
        this.refreshJvm = settings.refreshJvm;
        this.javaVendor = settings.javaVendor;
        this.javaVersion = settings.javaVersion;
        this.yourkitConfig = settings.yourkitConfig;
        this.profiler = settings.profiler;
        this.hprofSettings = settings.hprofSettings;
    }

    public int totalWorkerCount() {
        return memberWorkerCount + clientWorkerCount + mixedWorkerCount;
    }

    @Override
    public String toString() {
        return "WorkerSettings{" +
                "\n  vmOptions='" + vmOptions + '\'' +
                "\n  clientVmOptions='" + clientVmOptions + '\'' +
                "\n, memberWorkerCount=" + memberWorkerCount +
                "\n, clientWorkerCount=" + clientWorkerCount +
                "\n, mixedWorkerCount=" + mixedWorkerCount +
                "\n, workerStartupTimeout=" + workerStartupTimeout +
                "\n, refreshJvm=" + refreshJvm +
                "\n, profiler='" + profiler + '\'' +
                "\n, yourkitConfig='" + yourkitConfig + '\'' +
                "\n, javaVendor=" + javaVendor +
                "\n, javaVersion=" + javaVersion +
                "\n, hzConfig='" + hzConfig + '\'' +
                "\n, clientHzConfig='" + clientHzConfig + '\'' +
                "\n, hprofSettings='" + hprofSettings + '\'' +
                "\n}";
    }
}
