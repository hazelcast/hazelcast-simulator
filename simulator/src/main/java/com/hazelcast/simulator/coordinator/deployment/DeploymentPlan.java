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
import com.hazelcast.simulator.protocol.registry.WorkerData;
import com.hazelcast.simulator.utils.CommandLineExitException;
import com.hazelcast.simulator.worker.WorkerType;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.simulator.coordinator.deployment.ClusterConfiguration.getClusterConfiguration;
import static com.hazelcast.simulator.utils.FormatUtils.HORIZONTAL_RULER;
import static com.hazelcast.simulator.utils.FormatUtils.formatIpAddress;
import static com.hazelcast.simulator.utils.FormatUtils.formatLong;
import static com.hazelcast.simulator.utils.FormatUtils.padLeft;
import static java.lang.String.format;

public final class DeploymentPlan {
    private static final int WORKER_MODE_LENGTH = 6;

    private static final Logger LOGGER = Logger.getLogger(DeploymentPlan.class);

    private final Map<SimulatorAddress, List<WorkerProcessSettings>> workerDeployment
            = new HashMap<SimulatorAddress, List<WorkerProcessSettings>>();
    private final List<AgentWorkerLayout> agentWorkerLayouts = new ArrayList<AgentWorkerLayout>();

    public DeploymentPlan() {
    }

    public static DeploymentPlan createDeploymentPlanFromClusterXml(ComponentRegistry componentRegistry,
                                                                    Map<WorkerType, WorkerParameters> workerParametersMap,
                                                                    SimulatorProperties properties,
                                                                    int defaultHzPort,
                                                                    String licenseKey,
                                                                    String clusterXml) {
        WorkerConfigurationConverter workerConfigurationConverter = new WorkerConfigurationConverter(
                defaultHzPort, licenseKey, workerParametersMap, properties, componentRegistry);

        return generateFromXml(componentRegistry, workerParametersMap, workerConfigurationConverter, clusterXml);
    }

    static DeploymentPlan generateFromXml(ComponentRegistry componentRegistry,
                                          Map<WorkerType, WorkerParameters> parametersMap,
                                          WorkerConfigurationConverter converter,
                                          String clusterXml) {
        DeploymentPlan deploymentPlan = new DeploymentPlan();

        deploymentPlan.initAgentWorkerLayouts(componentRegistry);
        int agentCount = componentRegistry.agentCount();
        ClusterConfiguration clusterConfiguration = getClusterConfiguration(converter, clusterXml);
        if (clusterConfiguration.size() != agentCount) {
            throw new CommandLineExitException(format("Found %d node configurations for %d agents (number must be equal)",
                    clusterConfiguration.size(), agentCount));
        }

        Iterator<AgentWorkerLayout> iterator = deploymentPlan.agentWorkerLayouts.iterator();
        for (NodeConfiguration nodeConfiguration : clusterConfiguration.getNodeConfigurations()) {
            AgentWorkerLayout agentWorkerLayout = iterator.next();
            SimulatorAddress agentAddress = agentWorkerLayout.agentData.getAddress();
            for (WorkerGroup workerGroup : nodeConfiguration.getWorkerGroups()) {
                WorkerConfiguration workerConfig = clusterConfiguration.getWorkerConfiguration(workerGroup.getConfiguration());
                for (int i = 0; i < workerGroup.getCount(); i++) {
                    WorkerType workerType = workerConfig.getType();
                    WorkerProcessSettings settings = agentWorkerLayout.addWorker(workerType, parametersMap.get(workerType));
                    deploymentPlan.workerDeployment.get(agentAddress).add(settings);
                }
            }
            agentWorkerLayout.agentWorkerMode = AgentWorkerMode.CUSTOM;
        }

        deploymentPlan.printLayout("cluster.xml");

        return deploymentPlan;
    }

    public static DeploymentPlan createDeploymentPlan(
            ComponentRegistry componentRegistry,
            Map<WorkerType, WorkerParameters> workerParametersMap,
            int memberWorkerCount,
            int clientWorkerCount,
            int dedicatedMemberMachineCount) {
        checkParameters(componentRegistry.agentCount(), dedicatedMemberMachineCount, memberWorkerCount, clientWorkerCount);

        // if the memberCount has not been specified; we need to calculate it on the fly.
        if (memberWorkerCount == -1) {
            memberWorkerCount = dedicatedMemberMachineCount == 0
                    ? componentRegistry.agentCount()
                    : dedicatedMemberMachineCount;
        }

        DeploymentPlan plan = new DeploymentPlan();

        plan.initAgentWorkerLayouts(componentRegistry);
        int agentCount = componentRegistry.agentCount();

        plan.assignDedicatedMemberMachines(agentCount, dedicatedMemberMachineCount);

        AtomicInteger agentIndex = new AtomicInteger(plan.getStartIndex());
        plan.assign(memberWorkerCount, agentIndex, AgentWorkerMode.CLIENT,
                WorkerType.MEMBER, workerParametersMap.get(WorkerType.MEMBER));
        plan.assign(clientWorkerCount, agentIndex, AgentWorkerMode.MEMBER,
                WorkerType.CLIENT, workerParametersMap.get(WorkerType.CLIENT));
        plan.printLayout("arguments");
        return plan;
    }

    // just for testing
    public static DeploymentPlan createSingleInstanceDeploymentPlan(String agentIpAddress, WorkerParameters workerParameters) {
        AgentData agentData = new AgentData(1, agentIpAddress, agentIpAddress);
        AgentWorkerLayout agentWorkerLayout = new AgentWorkerLayout(agentData, AgentWorkerMode.MEMBER);
        agentWorkerLayout.addWorker(WorkerType.MEMBER, workerParameters);
        DeploymentPlan deploymentPlan = new DeploymentPlan();
        deploymentPlan.workerDeployment.put(agentData.getAddress(), agentWorkerLayout.workerProcessSettingsList);
        return deploymentPlan;
    }

    private void assign(int workerCount, AtomicInteger agentIndex, AgentWorkerMode member,
                        WorkerType workerType, WorkerParameters parameters) {
        for (int i = 0; i < workerCount; i++) {
            AgentWorkerLayout agentWorkerLayout = findNextAgentLayout(agentIndex, member);
            WorkerProcessSettings workerProcessSettings = agentWorkerLayout.addWorker(
                    workerType, parameters);
            List<WorkerProcessSettings> processSettingsList = workerDeployment.get(agentWorkerLayout.agentData.getAddress());
            processSettingsList.add(workerProcessSettings);
        }
    }

    private static void checkParameters(int agentCount, int dedicatedMemberMachineCount,
                                        int memberWorkerCount, int clientWorkerCount) {
        if (agentCount == 0) {
            throw new CommandLineExitException("You need at least one agent in your cluster!"
                    + " Please configure your agents.txt or run Provisioner.");
        }
        if (dedicatedMemberMachineCount < 0) {
            throw new CommandLineExitException("dedicatedMemberMachineCount can't be smaller than 0");
        }
        if (dedicatedMemberMachineCount > agentCount) {
            throw new CommandLineExitException(format("dedicatedMemberMachineCount %d can't be larger than number of agents %d",
                    dedicatedMemberMachineCount, agentCount));
        }
        if (clientWorkerCount > 0 && agentCount - dedicatedMemberMachineCount < 1) {
            throw new CommandLineExitException(
                    "dedicatedMemberMachineCount is too big, there are no machines left for clients!");
        }
        if (memberWorkerCount == 0 && clientWorkerCount == 0) {
            throw new CommandLineExitException("No workers have been defined!");
        }
    }

    private void initAgentWorkerLayouts(ComponentRegistry componentRegistry) {
        for (AgentData agentData : componentRegistry.getAgents()) {
            AgentWorkerLayout layout = new AgentWorkerLayout(agentData, AgentWorkerMode.MIXED);
            for (WorkerData workerData : agentData.getWorkers()) {
                layout.addWorker(workerData.getSettings());
            }
            agentWorkerLayouts.add(layout);

            workerDeployment.put(agentData.getAddress(), new ArrayList<WorkerProcessSettings>());
        }
    }

    private int getStartIndex() {
        int currentIndex = 0;
        int lastIndex = 0;
        int lastSize = Integer.MAX_VALUE;
        for (AgentWorkerLayout agentWorkerLayout : agentWorkerLayouts) {
            int workerCount = agentWorkerLayout.workerProcessSettingsList.size();
            if (workerCount < lastSize) {
                lastSize = workerCount;
                lastIndex = currentIndex;
            }
            currentIndex++;
        }
        return lastIndex;
    }

    private void assignDedicatedMemberMachines(int agentCount, int dedicatedMemberMachineCount) {
        if (dedicatedMemberMachineCount > 0) {
            assignAgentWorkerMode(0, dedicatedMemberMachineCount, AgentWorkerMode.MEMBER);
            assignAgentWorkerMode(dedicatedMemberMachineCount, agentCount, AgentWorkerMode.CLIENT);
        }
    }

    private void assignAgentWorkerMode(int startIndex, int endIndex,
                                       AgentWorkerMode agentWorkerMode) {
        for (int i = startIndex; i < endIndex; i++) {
            agentWorkerLayouts.get(i).agentWorkerMode = agentWorkerMode;
        }
    }

    private AgentWorkerLayout findNextAgentLayout(AtomicInteger currentIndex, AgentWorkerMode excludedAgentWorkerMode) {
        int size = agentWorkerLayouts.size();
        while (true) {
            AgentWorkerLayout agentLayout = agentWorkerLayouts.get(currentIndex.getAndIncrement() % size);
            if (agentLayout.agentWorkerMode != excludedAgentWorkerMode) {
                return agentLayout;
            }
        }
    }

    private void printLayout(String layoutType) {
        LOGGER.info(HORIZONTAL_RULER);
        LOGGER.info("Cluster layout");
        LOGGER.info(HORIZONTAL_RULER);

        LOGGER.info(format("Created via %s: ", layoutType));
        for (AgentWorkerLayout agentWorkerLayout : agentWorkerLayouts) {
            Set<String> agentVersionSpecs = agentWorkerLayout.getVersionSpecs();
            int agentMemberWorkerCount = agentWorkerLayout.getCount(WorkerType.MEMBER);
            int agentClientWorkerCount = agentWorkerLayout.getCount(WorkerType.CLIENT);
            int totalWorkerCount = agentMemberWorkerCount + agentClientWorkerCount;

            String message = "    Agent %s (%s) members: %s, clients: %s";
            if (totalWorkerCount > 0) {
                message += ", mode: %s, version specs: %s";
            } else {
                message += " (no workers)";
            }
            LOGGER.info(format(message,
                    agentWorkerLayout.formatIpAddresses(),
                    agentWorkerLayout.agentData.getAddress(),
                    formatLong(agentMemberWorkerCount, 2),
                    formatLong(agentClientWorkerCount, 2),
                    padLeft(agentWorkerLayout.agentWorkerMode.toString(), WORKER_MODE_LENGTH),
                    agentVersionSpecs
            ));
        }
    }

    public Set<String> getVersionSpecs() {
        Set<String> result = new HashSet<String>();
        for (AgentWorkerLayout agentWorkerLayout : agentWorkerLayouts) {
            result.addAll(agentWorkerLayout.getVersionSpecs());
        }

        return result;
    }

    public Map<SimulatorAddress, List<WorkerProcessSettings>> getWorkerDeployment() {
        return workerDeployment;
    }

    public int getMemberWorkerCount() {
        int result = 0;
        for (AgentWorkerLayout agentWorkerLayout : agentWorkerLayouts) {
            for (WorkerProcessSettings workerProcessSettings : agentWorkerLayout.workerProcessSettingsList) {
                if (workerProcessSettings.getWorkerType().isMember()) {
                    result++;
                }
            }
        }

        return result;
    }

    public int getClientWorkerCount() {
        int result = 0;
        for (AgentWorkerLayout agentWorkerLayout : agentWorkerLayouts) {
            for (WorkerProcessSettings workerProcessSettings : agentWorkerLayout.workerProcessSettingsList) {
                if (!workerProcessSettings.getWorkerType().isMember()) {
                    result++;
                }
            }
        }

        return result;
    }

    enum AgentWorkerMode {
        MEMBER,
        CLIENT,
        MIXED,
        CUSTOM
    }

    /**
     * The layout of Simulator Workers for a given Simulator Agent.
     */
    static final class AgentWorkerLayout {

        final List<WorkerProcessSettings> workerProcessSettingsList = new ArrayList<WorkerProcessSettings>();

        final AgentData agentData;

        AgentWorkerMode agentWorkerMode;

        AgentWorkerLayout(AgentData agentData, AgentWorkerMode agentWorkerMode) {
            this.agentData = agentData;
            this.agentWorkerMode = agentWorkerMode;
        }

        Set<String> getVersionSpecs() {
            Set<String> result = new HashSet<String>();
            for (WorkerProcessSettings workerProcessSettings : workerProcessSettingsList) {
                result.add(workerProcessSettings.getVersionSpec());
            }
            return result;
        }

        WorkerProcessSettings addWorker(WorkerType type, WorkerParameters parameters) {
            WorkerProcessSettings settings = new WorkerProcessSettings(
                    agentData.getNextWorkerIndex(),
                    type,
                    parameters.getVersionSpec(),
                    parameters.getWorkerScript(),
                    parameters.getWorkerStartupTimeout(),
                    parameters.getEnvironment());
            workerProcessSettingsList.add(settings);

            return settings;
        }

        void addWorker(WorkerProcessSettings settings) {
            agentData.getNextWorkerIndex();
            workerProcessSettingsList.add(settings);
        }

        int getCount(WorkerType type) {
            int count = 0;
            for (WorkerProcessSettings workerProcessSettings : workerProcessSettingsList) {
                if (workerProcessSettings.getWorkerType() == type) {
                    count++;
                }
            }
            return count;
        }

        String formatIpAddresses() {
            String publicIp = formatIpAddress(agentData.getPublicAddress());
            String privateIp = formatIpAddress(agentData.getPrivateAddress());
            if (publicIp.equals(privateIp)) {
                return publicIp;
            }
            return publicIp + " " + privateIp;
        }
    }
}
