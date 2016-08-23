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

import com.hazelcast.simulator.agent.workerprocess.WorkerProcessSettings;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.registry.AgentData;
import com.hazelcast.simulator.protocol.registry.ComponentRegistry;
import com.hazelcast.simulator.protocol.registry.WorkerData;
import com.hazelcast.simulator.utils.CommandLineExitException;
import com.hazelcast.simulator.common.WorkerType;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

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
    private final AtomicInteger agentIndex = new AtomicInteger();

    public static DeploymentPlan createDeploymentPlan(
            ComponentRegistry componentRegistry,
            Map<WorkerType, WorkerParameters> workerParametersMap,
            WorkerType clientType,
            int memberCount,
            int clientCount,
            int dedicatedMemberMachineCount) {
        checkParameters(componentRegistry.agentCount(), dedicatedMemberMachineCount, memberCount, clientCount);

        // if the memberCount has not been specified; we need to calculate it on the fly.
        if (memberCount == -1) {
            memberCount = dedicatedMemberMachineCount == 0
                    ? componentRegistry.agentCount()
                    : dedicatedMemberMachineCount;
        }

        DeploymentPlan plan = new DeploymentPlan();

        plan.initAgentWorkerLayouts(componentRegistry);

        plan.assignDedicatedMemberMachines(componentRegistry.agentCount(), dedicatedMemberMachineCount);

        plan.initAgentIndex();

        plan.assign(memberCount, WorkerType.MEMBER, workerParametersMap.get(WorkerType.MEMBER));

        plan.assign(clientCount, clientType, workerParametersMap.get(clientType));

        plan.printLayout("arguments");

        return plan;
    }

    public static DeploymentPlan createDeploymentPlan(ComponentRegistry componentRegistry,
                                                      WorkerParameters workerParameters,
                                                      WorkerType workerType,
                                                      int workerCount,
                                                      int dedicatedMemberWorker) {
        DeploymentPlan plan = new DeploymentPlan();

        plan.initAgentWorkerLayouts(componentRegistry);

        plan.assignDedicatedMemberMachines(componentRegistry.agentCount(), dedicatedMemberWorker);

        plan.initAgentIndex();

        plan.assign(workerCount, workerType, workerParameters);

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

    private void assign(int workerCount, WorkerType workerType, WorkerParameters parameters) {

        for (int i = 0; i < workerCount; i++) {
            AgentWorkerLayout agentWorkerLayout = nextAgent(workerType);

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

    private void initAgentIndex() {
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

        agentIndex.set(lastIndex);
    }

    private void assignDedicatedMemberMachines(int agentCount, int dedicatedMemberMachineCount) {
        if (dedicatedMemberMachineCount > 0) {
            assignAgentWorkerMode(0, dedicatedMemberMachineCount, AgentWorkerMode.MEMBER);
            assignAgentWorkerMode(dedicatedMemberMachineCount, agentCount, AgentWorkerMode.CLIENT);
        }
    }

    private void assignAgentWorkerMode(int startIndex, int endIndex, AgentWorkerMode agentWorkerMode) {
        for (int i = startIndex; i < endIndex; i++) {
            agentWorkerLayouts.get(i).agentWorkerMode = agentWorkerMode;
        }
    }

    private AgentWorkerLayout nextAgent(WorkerType workerType) {
        int size = agentWorkerLayouts.size();
        while (true) {
            AgentWorkerLayout agentLayout = agentWorkerLayouts.get(agentIndex.getAndIncrement() % size);
            if (agentLayout.allowed(workerType)) {
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
            int agentClientWorkerCount = agentWorkerLayout.workerProcessSettingsList.size() - agentMemberWorkerCount;
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
        MIXED
    }

    /**
     * The layout of Simulator Workers for a given Simulator Agent.
     */
    static final class AgentWorkerLayout {

        final List<WorkerProcessSettings> workerProcessSettingsList = new ArrayList<WorkerProcessSettings>();

        final AgentData agentData;

        AgentWorkerMode agentWorkerMode = AgentWorkerMode.MIXED;

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

        boolean allowed(WorkerType workerType) {
            switch (agentWorkerMode) {
                case MEMBER:
                    return workerType.isMember();
                case CLIENT:
                    return !workerType.isMember();
                case MIXED:
                    return true;
                default:
                    throw new RuntimeException("Unhandled agentWorkerMode:" + agentWorkerMode);
            }
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
                if (workerProcessSettings.getWorkerType().equals(type)) {
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
