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
import com.hazelcast.simulator.test.TestPhase;
import com.hazelcast.simulator.utils.CommandLineExitException;

import java.io.File;

import static java.lang.Boolean.parseBoolean;

/**
 * Parameters for Simulator Coordinator.
 */
public class CoordinatorParameters {

    private final SimulatorProperties simulatorProperties;
    private final File agentsFile;
    private final String workerClassPath;

    private final boolean monitorPerformance;
    private final boolean verifyEnabled;
    private final boolean parallel;
    private final TestPhase lastTestPhaseToSync;

    private final String log4jConfig;
    private final String memberJvmOptions;
    private final String clientJvmOptions;

    private final int workerStartupTimeout;
    private final boolean autoCreateHzInstance;
    private final boolean refreshJvm;
    private final boolean passiveMembers;

    private final JavaProfiler profiler;
    private final String profilerSettings;
    private final String numaCtl;

    private final int dedicatedMemberMachineCount;
    private final int clientWorkerCount;

    private int memberWorkerCount;

    private String memberHzConfig;
    private String clientHzConfig;

    @SuppressWarnings({"checkstyle:executablestatementcount", "checkstyle:parameternumber"})
    public CoordinatorParameters(SimulatorProperties props, File agentsFile, String workerClassPath, boolean monitorPerformance,
                                 boolean verifyEnabled, boolean parallel, TestPhase lastTestPhaseToSync, String log4jConfig,
                                 String memberJvmOptions, String clientJvmOptions, int workerStartupTimeout,
                                 boolean autoCreateHzInstance, boolean refreshJvm, int dedicatedMemberMachineCount,
                                 int memberWorkerCount, int clientWorkerCount, String memberHzConfig, String clientHzConfig) {
        if (dedicatedMemberMachineCount < 0) {
            throw new CommandLineExitException("--dedicatedMemberMachines can't be smaller than 0");
        }

        this.simulatorProperties = props;
        this.agentsFile = agentsFile;
        this.workerClassPath = workerClassPath;
        this.monitorPerformance = monitorPerformance;
        this.verifyEnabled = verifyEnabled;
        this.parallel = parallel;
        this.lastTestPhaseToSync = lastTestPhaseToSync;

        this.log4jConfig = log4jConfig;
        this.memberJvmOptions = memberJvmOptions;
        this.clientJvmOptions = clientJvmOptions;

        this.workerStartupTimeout = workerStartupTimeout;
        this.autoCreateHzInstance = autoCreateHzInstance;
        this.refreshJvm = refreshJvm;
        this.passiveMembers = parseBoolean(simulatorProperties.get("PASSIVE_MEMBERS", "true"));

        this.profiler = initProfiler();
        this.profilerSettings = initProfilerSettings();
        this.numaCtl = simulatorProperties.get("NUMA_CONTROL", "none");

        this.dedicatedMemberMachineCount = dedicatedMemberMachineCount;
        this.clientWorkerCount = clientWorkerCount;

        this.memberWorkerCount = memberWorkerCount;

        this.memberHzConfig = memberHzConfig;
        this.clientHzConfig = clientHzConfig;
    }

    private JavaProfiler initProfiler() {
        return JavaProfiler.valueOf(simulatorProperties.get("PROFILER", JavaProfiler.NONE.name()).toUpperCase());
    }

    private String initProfilerSettings() {
        switch (profiler) {
            case YOURKIT:
            case FLIGHTRECORDER:
                return simulatorProperties.get(profiler.name() + "_SETTINGS");
            case HPROF:
            case PERF:
            case VTUNE:
                return simulatorProperties.get(profiler.name() + "_SETTINGS", "");
            default:
                return "";
        }
    }

    public SimulatorProperties getSimulatorProperties() {
        return simulatorProperties;
    }

    public File getAgentsFile() {
        return agentsFile;
    }

    public String getWorkerClassPath() {
        return workerClassPath;
    }

    public boolean isMonitorPerformance() {
        return monitorPerformance;
    }

    public boolean isVerifyEnabled() {
        return verifyEnabled;
    }

    public boolean isParallel() {
        return parallel;
    }

    public TestPhase getLastTestPhaseToSync() {
        return lastTestPhaseToSync;
    }

    public String getMemberJvmOptions() {
        return memberJvmOptions;
    }

    public String getClientJvmOptions() {
        return clientJvmOptions;
    }

    public String getLog4jConfig() {
        return log4jConfig;
    }

    public int getWorkerStartupTimeout() {
        return workerStartupTimeout;
    }

    public boolean isRefreshJvm() {
        return refreshJvm;
    }

    public boolean isAutoCreateHzInstance() {
        return autoCreateHzInstance;
    }

    public boolean isPassiveMembers() {
        return passiveMembers;
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

    public int getDedicatedMemberMachineCount() {
        return dedicatedMemberMachineCount;
    }

    public int getClientWorkerCount() {
        return clientWorkerCount;
    }

    public int getMemberWorkerCount() {
        return memberWorkerCount;
    }

    public void setMemberWorkerCount(int memberWorkerCount) {
        this.memberWorkerCount = memberWorkerCount;
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
}
