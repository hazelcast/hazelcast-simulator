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
package com.hazelcast.simulator.coordinator.deployment;

import com.hazelcast.simulator.agent.workerprocess.WorkerProcessSettings;
import com.hazelcast.simulator.common.SimulatorProperties;
import com.hazelcast.simulator.coordinator.WorkerParameters;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.registry.AgentData;
import com.hazelcast.simulator.protocol.registry.ComponentRegistry;
import com.hazelcast.simulator.worker.WorkerType;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.simulator.coordinator.deployment.DeploymentUtils.generateFromArguments;
import static com.hazelcast.simulator.coordinator.deployment.DeploymentUtils.generateFromXml;

public final class DeploymentPlan {

    private final Set<String> versionSpecs = new HashSet<String>();

    private final Map<SimulatorAddress, List<WorkerProcessSettings>> workerDeployment;
    private final int memberWorkerCount;
    private final int clientWorkerCount;

    private DeploymentPlan(Map<SimulatorAddress, List<WorkerProcessSettings>> workerDeployment) {
        int tmpMemberWorkerCount = 0;
        int tmpClientWorkerCount = 0;
        for (List<WorkerProcessSettings> workerProcessSettingList : workerDeployment.values()) {
            for (WorkerProcessSettings workerProcessSettings : workerProcessSettingList) {
                versionSpecs.add(workerProcessSettings.getVersionSpec());
                if (workerProcessSettings.getWorkerType().isMember()) {
                    tmpMemberWorkerCount++;
                } else {
                    tmpClientWorkerCount++;
                }
            }
        }

        this.workerDeployment = workerDeployment;
        this.memberWorkerCount = tmpMemberWorkerCount;
        this.clientWorkerCount = tmpClientWorkerCount;
    }

    public static DeploymentPlan createDeploymentPlanFromClusterXml(ComponentRegistry componentRegistry,
                                                                    Map<WorkerType, WorkerParameters> workerParametersMap,
                                                                    SimulatorProperties properties,
                                                                    int defaultHzPort,
                                                                    String licenseKey,
                                                                    String clusterXml) {
        WorkerConfigurationConverter workerConfigurationConverter = new WorkerConfigurationConverter(
                defaultHzPort, licenseKey, workerParametersMap, properties, componentRegistry);
        return new DeploymentPlan(
                generateFromXml(componentRegistry, workerParametersMap, workerConfigurationConverter, clusterXml));
    }

    public static DeploymentPlan createDeploymentPlan(ComponentRegistry componentRegistry,
                                                      Map<WorkerType, WorkerParameters> workerParametersMap,
                                                      int memberWorker,
                                                      int clientWorker,
                                                      int dedicatedMemberWorker) {
        Map<SimulatorAddress, List<WorkerProcessSettings>> workerDeployment = generateFromArguments(
                componentRegistry, workerParametersMap, memberWorker, clientWorker, dedicatedMemberWorker);
        return new DeploymentPlan(workerDeployment);
    }

    // just for testing
    public static DeploymentPlan createSingleInstanceDeploymentPlan(String agentIpAddress, WorkerParameters workerParameters) {
        AgentData agentData = new AgentData(1, agentIpAddress, agentIpAddress);
        AgentWorkerLayout agentWorkerLayout = new AgentWorkerLayout(agentData, AgentWorkerMode.MEMBER);
        agentWorkerLayout.addWorker(WorkerType.MEMBER, workerParameters);

        Map<SimulatorAddress, List<WorkerProcessSettings>> workerDeployment
                = new HashMap<SimulatorAddress, List<WorkerProcessSettings>>();
        workerDeployment.put(agentData.getAddress(), agentWorkerLayout.getWorkerProcessSettings());

        return new DeploymentPlan(workerDeployment);
    }

    // just for testing
    public static DeploymentPlan createEmptyDeploymentPlan() {
        return new DeploymentPlan(new HashMap<SimulatorAddress, List<WorkerProcessSettings>>());
    }

    public Set<String> getVersionSpecs() {
        return versionSpecs;
    }

    public Map<SimulatorAddress, List<WorkerProcessSettings>> getWorkerDeployment() {
        return workerDeployment;
    }

    public int getMemberWorkerCount() {
        return memberWorkerCount;
    }

    public int getClientWorkerCount() {
        return clientWorkerCount;
    }
}
