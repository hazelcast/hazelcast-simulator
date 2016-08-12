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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.simulator.coordinator.deployment.ClusterConfigurationUtils.fromXml;
import static com.hazelcast.simulator.utils.FormatUtils.HORIZONTAL_RULER;
import static com.hazelcast.simulator.utils.FormatUtils.formatIpAddress;
import static com.hazelcast.simulator.utils.FormatUtils.formatLong;
import static com.hazelcast.simulator.utils.FormatUtils.padLeft;
import static java.lang.String.format;

final class DeploymentUtils {

    private static final int WORKER_MODE_LENGTH = 6;

    private static final Logger LOGGER = Logger.getLogger(DeploymentPlan.class);

    private DeploymentUtils() {
    }

    static String formatIpAddresses(AgentWorkerLayout agentWorkerLayout) {
        String publicIp = formatIpAddress(agentWorkerLayout.getPublicAddress());
        String privateIp = formatIpAddress(agentWorkerLayout.getPrivateAddress());
        if (publicIp.equals(privateIp)) {
            return publicIp;
        }
        return publicIp + " " + privateIp;
    }

    static Map<SimulatorAddress, List<WorkerProcessSettings>> generateFromXml(ComponentRegistry componentRegistry,
                                                                              WorkerParameters parameters,
                                                                              WorkerConfigurationConverter converter,
                                                                              String clusterXml) {
        Map<SimulatorAddress, List<WorkerProcessSettings>> workerDeployment
                = new HashMap<SimulatorAddress, List<WorkerProcessSettings>>();

        List<AgentWorkerLayout> agentWorkerLayouts = initAgentWorkerLayouts(componentRegistry, workerDeployment);
        int agentCount = componentRegistry.agentCount();
        ClusterConfiguration clusterConfiguration = getClusterConfiguration(converter, clusterXml);
        if (clusterConfiguration.size() != agentCount) {
            throw new CommandLineExitException(format("Found %d node configurations for %d agents (number must be equal)",
                    clusterConfiguration.size(), agentCount));
        }

        Iterator<AgentWorkerLayout> iterator = agentWorkerLayouts.iterator();
        for (NodeConfiguration nodeConfiguration : clusterConfiguration.getNodeConfigurations()) {
            AgentWorkerLayout agentWorkerLayout = iterator.next();
            SimulatorAddress agentAddress = agentWorkerLayout.getSimulatorAddress();
            for (WorkerGroup workerGroup : nodeConfiguration.getWorkerGroups()) {
                WorkerConfiguration workerConfig = clusterConfiguration.getWorkerConfiguration(workerGroup.getConfiguration());
                for (int i = 0; i < workerGroup.getCount(); i++) {
                    WorkerProcessSettings settings = agentWorkerLayout.addWorker(workerConfig.getType(), parameters);
                    workerDeployment.get(agentAddress).add(settings);
                }
            }
            agentWorkerLayout.setAgentWorkerMode(AgentWorkerMode.CUSTOM);
        }

        printLayout(agentWorkerLayouts, "cluster.xml");

        return workerDeployment;
    }

    private static ClusterConfiguration getClusterConfiguration(WorkerConfigurationConverter converter,
                                                                String clusterConfiguration) {
        try {
            return fromXml(converter, clusterConfiguration);
        } catch (Exception e) {
            throw new CommandLineExitException("Could not parse cluster configuration", e);
        }
    }

    static Map<SimulatorAddress, List<WorkerProcessSettings>> generateFromArguments(ComponentRegistry componentRegistry,
                                                                                    WorkerParameters parameters,
                                                                                    int memberWorkerCount,
                                                                                    int clientWorkerCount,
                                                                                    int dedicatedMemberMachineCount) {
        checkParameters(componentRegistry.agentCount(), dedicatedMemberMachineCount, memberWorkerCount, clientWorkerCount);

        Map<SimulatorAddress, List<WorkerProcessSettings>> workerDeployment
                = new HashMap<SimulatorAddress, List<WorkerProcessSettings>>();

        List<AgentWorkerLayout> agentWorkerLayouts = initAgentWorkerLayouts(componentRegistry, workerDeployment);
        int agentCount = componentRegistry.agentCount();

        assignDedicatedMemberMachines(agentCount, agentWorkerLayouts, dedicatedMemberMachineCount);

        AtomicInteger currentIndex = new AtomicInteger(getStartIndex(agentWorkerLayouts));
        for (int i = 0; i < memberWorkerCount; i++) {
            // assign members
            AgentWorkerLayout agentWorkerLayout = findNextAgentLayout(currentIndex, agentWorkerLayouts, AgentWorkerMode.CLIENT);
            WorkerProcessSettings workerProcessSettings = agentWorkerLayout.addWorker(WorkerType.MEMBER, parameters);
            workerDeployment.get(agentWorkerLayout.getSimulatorAddress()).add(workerProcessSettings);
        }
        for (int i = 0; i < clientWorkerCount; i++) {
            // assign clients
            AgentWorkerLayout agentWorkerLayout = findNextAgentLayout(currentIndex, agentWorkerLayouts, AgentWorkerMode.MEMBER);
            WorkerProcessSettings workerProcessSettings = agentWorkerLayout.addWorker(WorkerType.CLIENT, parameters);
            workerDeployment.get(agentWorkerLayout.getSimulatorAddress()).add(workerProcessSettings);
        }

        printLayout(agentWorkerLayouts, "arguments");

        return workerDeployment;
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

    private static List<AgentWorkerLayout> initAgentWorkerLayouts(ComponentRegistry componentRegistry,
                                                                  Map<SimulatorAddress, List<WorkerProcessSettings>>
                                                                          workerDeployment) {
        List<AgentWorkerLayout> agentWorkerLayouts = new LinkedList<AgentWorkerLayout>();
        for (AgentData agentData : componentRegistry.getAgents()) {
            AgentWorkerLayout layout = new AgentWorkerLayout(agentData, AgentWorkerMode.MIXED);
            for (WorkerData workerData : agentData.getWorkers()) {
                layout.addWorker(workerData.getSettings());
            }
            agentWorkerLayouts.add(layout);

            workerDeployment.put(agentData.getAddress(), new ArrayList<WorkerProcessSettings>());
        }
        return agentWorkerLayouts;
    }

    private static int getStartIndex(List<AgentWorkerLayout> agentWorkerLayouts) {
        int currentIndex = 0;
        int lastIndex = 0;
        int lastSize = Integer.MAX_VALUE;
        for (AgentWorkerLayout agentWorkerLayout : agentWorkerLayouts) {
            int workerCount = agentWorkerLayout.getWorkerProcessSettings().size();
            if (workerCount < lastSize) {
                lastSize = workerCount;
                lastIndex = currentIndex;
            }
            currentIndex++;
        }
        return lastIndex;
    }

    private static void assignDedicatedMemberMachines(int agentCount, List<AgentWorkerLayout> agentWorkerLayouts,
                                                      int dedicatedMemberMachineCount) {
        if (dedicatedMemberMachineCount > 0) {
            assignAgentWorkerMode(agentWorkerLayouts, 0, dedicatedMemberMachineCount, AgentWorkerMode.MEMBER);
            assignAgentWorkerMode(agentWorkerLayouts, dedicatedMemberMachineCount, agentCount, AgentWorkerMode.CLIENT);
        }
    }

    private static void assignAgentWorkerMode(List<AgentWorkerLayout> agentWorkerLayouts, int startIndex, int endIndex,
                                              AgentWorkerMode agentWorkerMode) {
        for (int i = startIndex; i < endIndex; i++) {
            agentWorkerLayouts.get(i).setAgentWorkerMode(agentWorkerMode);
        }
    }

    private static AgentWorkerLayout findNextAgentLayout(AtomicInteger currentIndex, List<AgentWorkerLayout> agentWorkerLayouts,
                                                         AgentWorkerMode excludedAgentWorkerMode) {
        int size = agentWorkerLayouts.size();
        while (true) {
            AgentWorkerLayout agentLayout = agentWorkerLayouts.get(currentIndex.getAndIncrement() % size);
            if (agentLayout.getAgentWorkerMode() != excludedAgentWorkerMode) {
                return agentLayout;
            }
        }
    }

    private static void printLayout(List<AgentWorkerLayout> agentWorkerLayouts, String layoutType) {
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
                    formatIpAddresses(agentWorkerLayout),
                    agentWorkerLayout.getSimulatorAddress(),
                    formatLong(agentMemberWorkerCount, 2),
                    formatLong(agentClientWorkerCount, 2),
                    padLeft(agentWorkerLayout.getAgentWorkerMode().toString(), WORKER_MODE_LENGTH),
                    agentVersionSpecs
            ));
        }
    }
}
