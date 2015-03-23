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
package com.hazelcast.simulator.agent.workerjvm;

import java.io.Serializable;

public class WorkerJvmSettings implements Serializable {
    public String vmOptions;
    public String clientVmOptions;
    public String hzConfig;
    public String clientHzConfig;
    public String log4jConfig;

    public int memberWorkerCount;
    public int clientWorkerCount;
    public boolean autoCreateHZInstances = true;

    public int workerStartupTimeout;
    public boolean refreshJvm;
    public String javaVendor;
    public String javaVersion;
    public String profiler = "none";
    public String yourkitConfig;
    public String hprofSettings = "";
    public String perfSettings = "";
    public String vtuneSettings = "";
    public String flightrecorderSettings = "";
    public String numaCtl = "none";

    public WorkerJvmSettings() {
    }

    public WorkerJvmSettings(WorkerJvmSettings settings) {
        this.vmOptions = settings.vmOptions;
        this.clientVmOptions = settings.clientVmOptions;
        this.hzConfig = settings.hzConfig;
        this.clientHzConfig = settings.clientHzConfig;
        this.log4jConfig = settings.log4jConfig;
        this.memberWorkerCount = settings.memberWorkerCount;
        this.clientWorkerCount = settings.clientWorkerCount;
        this.autoCreateHZInstances = settings.autoCreateHZInstances;
        this.workerStartupTimeout = settings.workerStartupTimeout;
        this.refreshJvm = settings.refreshJvm;
        this.javaVendor = settings.javaVendor;
        this.javaVersion = settings.javaVersion;
        this.yourkitConfig = settings.yourkitConfig;
        this.profiler = settings.profiler;
        this.hprofSettings = settings.hprofSettings;
        this.perfSettings = settings.perfSettings;
        this.vtuneSettings = settings.vtuneSettings;
        this.flightrecorderSettings = settings.flightrecorderSettings;
        this.numaCtl = settings.numaCtl;
    }

    public int totalWorkerCount() {
        return memberWorkerCount + clientWorkerCount;
    }

    @Override
    public String toString() {
        return "WorkerSettings{"
                + "\n  vmOptions='" + vmOptions + '\''
                + "\n  clientVmOptions='" + clientVmOptions + '\''
                + "\n, memberWorkerCount=" + memberWorkerCount
                + "\n, clientWorkerCount=" + clientWorkerCount
                + "\n, workerStartupTimeout=" + workerStartupTimeout
                + "\n, refreshJvm=" + refreshJvm
                + "\n, profiler='" + profiler + '\''
                + "\n, yourkitConfig='" + yourkitConfig + '\''
                + "\n, javaVendor=" + javaVendor
                + "\n, javaVersion=" + javaVersion
                + "\n, hzConfig='" + hzConfig + '\''
                + "\n, clientHzConfig='" + clientHzConfig + '\''
                + "\n, log4jConfig='" + log4jConfig + '\''
                + "\n, hprofSettings='" + hprofSettings + '\''
                + "\n, perfSettings='" + perfSettings + '\''
                + "\n, vtuneSettings='" + vtuneSettings + '\''
                + "\n, flightrecorderSettings='" + flightrecorderSettings + '\''
                + "\n, numaCtl='" + numaCtl + '\''
                + "\n}";
    }
}
