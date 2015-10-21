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
import com.hazelcast.simulator.protocol.registry.ComponentRegistry;

import static com.hazelcast.simulator.coordinator.CoordinatorUtils.createAddressConfig;
import static com.hazelcast.simulator.coordinator.CoordinatorUtils.getPort;
import static java.lang.String.format;

/**
 * Parameters for Simulator Worker.
 */
public class WorkerParameters {

    private static final int DEFAULT_WORKER_PERFORMANCE_MONITOR_INTERVAL_SECONDS = 10;

    private final boolean autoCreateHzInstance;
    private final int workerStartupTimeout;

    private final String hazelcastVersionSpec;

    private final String memberJvmOptions;
    private final String clientJvmOptions;

    private String memberHzConfig;
    private String clientHzConfig;
    private final String log4jConfig;

    private final boolean monitorPerformance;
    private final int workerPerformanceMonitorIntervalSeconds;

    private final JavaProfiler profiler;
    private final String profilerSettings;
    private final String numaCtl;

    public WorkerParameters(SimulatorProperties properties, boolean autoCreateHzInstance, int workerStartupTimeout,
                            String memberJvmOptions, String clientJvmOptions, String memberHzConfig, String clientHzConfig,
                            String log4jConfig, boolean monitorPerformance) {
        this.autoCreateHzInstance = autoCreateHzInstance;
        this.workerStartupTimeout = workerStartupTimeout;

        this.hazelcastVersionSpec = properties.getHazelcastVersionSpec();

        this.memberJvmOptions = memberJvmOptions;
        this.clientJvmOptions = clientJvmOptions;

        this.memberHzConfig = memberHzConfig;
        this.clientHzConfig = clientHzConfig;
        this.log4jConfig = log4jConfig;

        this.monitorPerformance = monitorPerformance;
        this.workerPerformanceMonitorIntervalSeconds = initWorkerPerformanceMonitorIntervalSeconds(properties);

        this.profiler = initProfiler(properties);
        this.profilerSettings = initProfilerSettings(properties);
        this.numaCtl = properties.get("NUMA_CONTROL", "none");
    }

    private int initWorkerPerformanceMonitorIntervalSeconds(SimulatorProperties properties) {
        String intervalSeconds = properties.get("WORKER_PERFORMANCE_MONITOR_INTERVAL_SECONDS");
        if (intervalSeconds == null || intervalSeconds.isEmpty()) {
            return DEFAULT_WORKER_PERFORMANCE_MONITOR_INTERVAL_SECONDS;
        }
        return Integer.parseInt(intervalSeconds);
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

    public boolean isAutoCreateHzInstance() {
        return autoCreateHzInstance;
    }

    public int getWorkerStartupTimeout() {
        return workerStartupTimeout;
    }

    public String getHazelcastVersionSpec() {
        return hazelcastVersionSpec;
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

    public String getClientHzConfig() {
        return clientHzConfig;
    }

    public String getLog4jConfig() {
        return log4jConfig;
    }

    public boolean isMonitorPerformance() {
        return monitorPerformance;
    }

    public int getWorkerPerformanceMonitorIntervalSeconds() {
        return workerPerformanceMonitorIntervalSeconds;
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

    void initMemberHzConfig(ComponentRegistry componentRegistry, SimulatorProperties properties) {
        String addressConfig = createAddressConfig("member", componentRegistry, getPort(memberHzConfig));

        memberHzConfig = memberHzConfig.replace("<!--MEMBERS-->", addressConfig);

        String manCenterURL = properties.get("MANAGEMENT_CENTER_URL").trim();
        if (!"none".equals(manCenterURL) && (manCenterURL.startsWith("http://") || manCenterURL.startsWith("https://"))) {
            String updateInterval = properties.get("MANAGEMENT_CENTER_UPDATE_INTERVAL").trim();
            String updateIntervalAttr = (updateInterval.isEmpty()) ? "" : " update-interval=\"" + updateInterval + '"';
            memberHzConfig = memberHzConfig.replace("<!--MANAGEMENT_CENTER_CONFIG-->",
                    format("<management-center enabled=\"true\"%s>%n        %s%n" + "    </management-center>%n",
                            updateIntervalAttr, manCenterURL));
        }
    }

    void initClientHzConfig(ComponentRegistry componentRegistry) {
        String addressConfig = createAddressConfig("address", componentRegistry, getPort(memberHzConfig));

        clientHzConfig = clientHzConfig.replace("<!--MEMBERS-->", addressConfig);
    }
}
