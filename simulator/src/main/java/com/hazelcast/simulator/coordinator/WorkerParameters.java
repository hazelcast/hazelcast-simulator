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
package com.hazelcast.simulator.coordinator;

import com.hazelcast.simulator.common.SimulatorProperties;
import com.hazelcast.simulator.protocol.registry.AgentData;
import com.hazelcast.simulator.protocol.registry.ComponentRegistry;

import java.util.Map;

import static java.lang.String.format;

/**
 * Parameters for Simulator Worker.
 */
public class WorkerParameters {

    private final int workerStartupTimeout;
    private final String versionSpec;
    // value of 1 or higher means enabled.
    private final String workerScript;
    private final Map<String, String> environment;

    public WorkerParameters(String versionSpec,
                            int workerStartupTimeout,
                            String workerScript,
                            Map<String, String> environment) {
        this.workerStartupTimeout = workerStartupTimeout;
        this.versionSpec = versionSpec;
        this.workerScript = workerScript;
        this.environment = environment;
    }

    public Map<String, String> getEnvironment() {
        return environment;
    }

    public int getWorkerStartupTimeout() {
        return workerStartupTimeout;
    }

    public String getVersionSpec() {
        return versionSpec;
    }

    public String getWorkerScript() {
        return workerScript;
    }

    public static String initMemberHzConfig(String memberHzConfig, ComponentRegistry componentRegistry, int port,
                                            String licenseKey, SimulatorProperties properties, boolean liteMember) {
        String addressConfig = createAddressConfig("member", componentRegistry, port);
        memberHzConfig = updateAddressAndLicenseKey(memberHzConfig, addressConfig, licenseKey);

        String manCenterURL = properties.get("MANAGEMENT_CENTER_URL");
        if (!"none".equals(manCenterURL) && (manCenterURL.startsWith("http://") || manCenterURL.startsWith("https://"))) {
            String updateInterval = properties.get("MANAGEMENT_CENTER_UPDATE_INTERVAL");
            String updateIntervalAttr = (updateInterval.isEmpty()) ? "" : " update-interval=\"" + updateInterval + '"';
            memberHzConfig = memberHzConfig.replace("<!--MANAGEMENT_CENTER_CONFIG-->",
                    format("<management-center enabled=\"true\"%s>%n        %s%n" + "    </management-center>%n",
                            updateIntervalAttr, manCenterURL));
        }

        if (liteMember) {
            memberHzConfig = memberHzConfig.replace("<!--LITE_MEMBER_CONFIG-->", "<lite-member enabled=\"true\"/>");
        }

        return memberHzConfig;
    }

    public static String initClientHzConfig(String clientHzConfig, ComponentRegistry componentRegistry, int port,
                                            String licenseKey) {
        String addressConfig = createAddressConfig("address", componentRegistry, port);
        return updateAddressAndLicenseKey(clientHzConfig, addressConfig, licenseKey);
    }

    static String createAddressConfig(String tagName, ComponentRegistry componentRegistry, int port) {
        StringBuilder members = new StringBuilder();
        for (AgentData agentData : componentRegistry.getAgents()) {
            String hostAddress = agentData.getPrivateAddress();
            members.append(format("<%s>%s:%d</%s>%n", tagName, hostAddress, port, tagName));
        }
        return members.toString();
    }

    private static String updateAddressAndLicenseKey(String hzConfig, String addressConfig, String licenseKey) {
        hzConfig = hzConfig.replace("<!--MEMBERS-->", addressConfig);
        if (licenseKey != null) {
            String licenseConfig = format("<license-key>%s</license-key>", licenseKey);
            hzConfig = hzConfig.replace("<!--LICENSE-KEY-->", licenseConfig);
        }
        return hzConfig;
    }
}
