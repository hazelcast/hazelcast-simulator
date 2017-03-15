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

import com.hazelcast.simulator.agent.workerprocess.WorkerParameters;
import com.hazelcast.simulator.coordinator.registry.AgentData;
import com.hazelcast.simulator.coordinator.registry.ComponentRegistry;
import com.hazelcast.simulator.coordinator.registry.WorkerData;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.utils.CommandLineExitException;
import com.hazelcast.simulator.vendors.VendorDriver;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.simulator.protocol.core.AddressLevel.WORKER;
import static com.hazelcast.simulator.utils.FormatUtils.HORIZONTAL_RULER;
import static com.hazelcast.simulator.utils.FormatUtils.formatLong;
import static com.hazelcast.simulator.utils.FormatUtils.padLeft;
import static java.lang.String.format;

public final class DeploymentPlan {
    private static final int WORKER_MODE_LENGTH = 6;

    private static final Logger LOGGER = Logger.getLogger(DeploymentPlan.class);

    private final Map<SimulatorAddress, List<WorkerParameters>> workerDeployment
            = new HashMap<SimulatorAddress, List<WorkerParameters>>();
    private final List<AgentWorkerLayout> agentWorkerLayouts = new ArrayList<AgentWorkerLayout>();

    public static DeploymentPlan createDeploymentPlan(
            ComponentRegistry componentRegistry,
            VendorDriver vendorDriver,
            String workerType,
            int memberCount,
            int clientCount) {

        int dedicatedMemberMachineCount = componentRegistry.getDedicatedMemberMachines();

        checkParameters(componentRegistry.agentCount(), dedicatedMemberMachineCount, memberCount, clientCount);

        // if the memberCount has not been specified; we need to calculate it on the fly.
        if (memberCount == -1) {
            memberCount = dedicatedMemberMachineCount == 0
                    ? componentRegistry.agentCount()
                    : dedicatedMemberMachineCount;
        }

        DeploymentPlan plan = new DeploymentPlan();

        plan.initAgentWorkerLayouts(componentRegistry);

        List<SimulatorAddress> agents = allAgents(componentRegistry);

        plan.assignToAgents(memberCount, "member", vendorDriver.loadWorkerParameters("member"), agents);

        plan.assignToAgents(clientCount, workerType, vendorDriver.loadWorkerParameters(workerType), agents);

        plan.printLayout();

        return plan;
    }

    private static List<SimulatorAddress> allAgents(ComponentRegistry componentRegistry) {
        List<SimulatorAddress> result = new ArrayList<SimulatorAddress>();
        for (AgentData agentData : componentRegistry.getAgents()) {
            result.add(agentData.getAddress());
        }
        return result;
    }

    public static DeploymentPlan createDeploymentPlan(
            ComponentRegistry componentRegistry,
            WorkerParameters workerParameters,
            String workerType,
            int workerCount,
            List<SimulatorAddress> targetAgents) {

        if (workerCount < 0) {
            throw new IllegalArgumentException("workerCount can't be smaller than 0");
        }

        DeploymentPlan plan = new DeploymentPlan();

        plan.initAgentWorkerLayouts(componentRegistry);

        plan.assignToAgents(workerCount, workerType, workerParameters, targetAgents);

        plan.printLayout();

        return plan;
    }

    private void assignToAgents(int workerCount,
                                String workerType,
                                WorkerParameters parameters,
                                List<SimulatorAddress> targetAgents) {
        for (int i = 0; i < workerCount; i++) {
            AgentWorkerLayout agentWorkerLayout = nextAgent(workerType, targetAgents);

            agentWorkerLayout.registerWorker(parameters);

            List<WorkerParameters> workerParametersList = workerDeployment.get(agentWorkerLayout.agent.getAddress());
            workerParametersList.add(parameters);
        }
    }

    private static void checkParameters(int agentCount, int dedicatedMemberMachineCount,
                                        int memberWorkerCount, int clientWorkerCount) {
        if (agentCount == 0) {
            throw new CommandLineExitException("You need at least one agent in your cluster!"
                    + " Please configure your agents.txt or run Provisioner.");
        }

        if (clientWorkerCount > 0 && agentCount - dedicatedMemberMachineCount < 1) {
            throw new CommandLineExitException(
                    "there are no machines left for clients!");
        }

        if (memberWorkerCount == 0 && clientWorkerCount == 0) {
            throw new CommandLineExitException("No workers have been defined!");
        }
    }

    private void initAgentWorkerLayouts(ComponentRegistry componentRegistry) {
        for (AgentData agentData : componentRegistry.getAgents()) {
            AgentWorkerLayout layout = new AgentWorkerLayout(agentData);
            for (WorkerData workerData : agentData.getWorkers()) {
                layout.workers.add(workerData.getParameters());
            }
            agentWorkerLayouts.add(layout);

            workerDeployment.put(agentData.getAddress(), new ArrayList<WorkerParameters>());
        }
    }

    private AgentWorkerLayout nextAgent(String workerType, List<SimulatorAddress> allowedAgents) {
        AgentWorkerLayout smallest = null;
        for (AgentWorkerLayout agent : agentWorkerLayouts) {

            if (!agent.allowed(workerType) || !allowedAgents.contains(agent.agent.getAddress())) {
                continue;
            }

            if (smallest == null) {
                smallest = agent;
                continue;
            }

            if (agent.workers.size() < smallest.workers.size()) {
                smallest = agent;
            }
        }

        if (smallest == null) {
            throw new IllegalStateException(format("No agent found for workerType [%s]", workerType));
        }

        return smallest;
    }

    private void printLayout() {
        LOGGER.info(HORIZONTAL_RULER);
        LOGGER.info("Cluster layout");
        LOGGER.info(HORIZONTAL_RULER);

        for (AgentWorkerLayout agentWorkerLayout : agentWorkerLayouts) {
            Set<String> agentVersionSpecs = agentWorkerLayout.getVersionSpecs();
            int agentMemberWorkerCount = agentWorkerLayout.count("member");
            int agentClientWorkerCount = agentWorkerLayout.workers.size() - agentMemberWorkerCount;
            int totalWorkerCount = agentMemberWorkerCount + agentClientWorkerCount;

            String message = "    Agent %s (%s) members: %s, clients: %s";
            if (totalWorkerCount > 0) {
                message += ", mode: %s, version specs: %s";
            } else {
                message += " (no workers)";
            }
            LOGGER.info(format(message,
                    agentWorkerLayout.agent.formatIpAddresses(),
                    agentWorkerLayout.agent.getAddress(),
                    formatLong(agentMemberWorkerCount, 2),
                    formatLong(agentClientWorkerCount, 2),
                    padLeft(agentWorkerLayout.agent.getAgentWorkerMode().toString(), WORKER_MODE_LENGTH),
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

    public Map<SimulatorAddress, List<WorkerParameters>> getWorkerDeployment() {
        return workerDeployment;
    }

    public int getMemberWorkerCount() {
        int result = 0;
        for (AgentWorkerLayout agentWorkerLayout : agentWorkerLayouts) {
            for (WorkerParameters workerParameters : agentWorkerLayout.workers) {
                if (workerParameters.getWorkerType().equals("member")) {
                    result++;
                }
            }
        }

        return result;
    }

    public int getClientWorkerCount() {
        int result = 0;
        for (AgentWorkerLayout agentWorkerLayout : agentWorkerLayouts) {
            for (WorkerParameters workerParameters : agentWorkerLayout.workers) {
                if (!workerParameters.getWorkerType().equals("member")) {
                    result++;
                }
            }
        }

        return result;
    }

    /**
     * The layout of Simulator Workers for a given Simulator Agent.
     */
    static final class AgentWorkerLayout {

        final List<WorkerParameters> workers = new ArrayList<WorkerParameters>();
        final AgentData agent;

        AgentWorkerLayout(AgentData agent) {
            this.agent = agent;
        }

        Set<String> getVersionSpecs() {
            Set<String> result = new HashSet<String>();
            for (WorkerParameters workerParameters : workers) {
                result.add(workerParameters.get("VERSION_SPEC"));
            }
            return result;
        }

        boolean allowed(String workerType) {
            switch (agent.getAgentWorkerMode()) {
                case MEMBERS_ONLY:
                    return workerType.equals("member");
                case CLIENTS_ONLY:
                    return !workerType.equals("member");
                case MIXED:
                    return true;
                default:
                    throw new RuntimeException("Unhandled agentWorkerMode: " + agent.getAgentWorkerMode());
            }
        }

        void registerWorker(WorkerParameters parameters) {
            int workerIndex = agent.getNextWorkerIndex();
            SimulatorAddress workerAddress = new SimulatorAddress(WORKER, agent.getAddressIndex(), workerIndex, 0);

            parameters.set("WORKER_ADDRESS", workerAddress)
                    .set("WORKER_INDEX", workerIndex)
                    .set("PUBLIC_IP", agent.getPublicAddress());
            workers.add(parameters);
        }

        int count(String type) {
            int count = 0;
            for (WorkerParameters workerParameters : workers) {
                if (workerParameters.getWorkerType().equals(type)) {
                    count++;
                }
            }
            return count;
        }
    }
}
