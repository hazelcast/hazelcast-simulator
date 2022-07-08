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
import com.hazelcast.simulator.coordinator.registry.Registry;
import com.hazelcast.simulator.coordinator.registry.WorkerData;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.utils.CommandLineExitException;
import com.hazelcast.simulator.drivers.Driver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.simulator.protocol.core.SimulatorAddress.workerAddress;
import static com.hazelcast.simulator.utils.FormatUtils.HORIZONTAL_RULER;
import static com.hazelcast.simulator.utils.FormatUtils.formatLong;
import static com.hazelcast.simulator.utils.FormatUtils.padLeft;
import static java.lang.String.format;

public final class DeploymentPlan {
    private static final int WORKER_MODE_LENGTH = 6;

    private static final Logger LOGGER = LogManager.getLogger(DeploymentPlan.class);

    private final Map<SimulatorAddress, List<WorkerParameters>> workerDeployment
            = new HashMap<>();
    private final List<WorkersPerAgent> workersPerAgentList = new ArrayList<>();

    private final Driver driver;

    public DeploymentPlan(Driver driver, Registry registry) {
        this(driver, registry.getAgents());
    }

    public DeploymentPlan(Driver driver, List<AgentData> agents) {
        this.driver = driver;

        if (agents.isEmpty()) {
            throw new CommandLineExitException("You need at least one agent in your cluster!"
                    + " Please configure your agents.txt or run Provisioner.");
        }

        for (AgentData agent : agents) {
            WorkersPerAgent workersPerAgent = new WorkersPerAgent(agent);
            for (WorkerData worker : agent.getWorkers()) {
                workersPerAgent.workers.add(worker.getParameters());
            }
            workersPerAgentList.add(workersPerAgent);
            workerDeployment.put(agent.getAddress(), new ArrayList<>());
        }
    }

    public DeploymentPlan addToPlan(int workerCount, String workerType) {
        for (int i = 0; i < workerCount; i++) {
            WorkersPerAgent workersPerAgent = nextAgent(workerType);
            AgentData agent = workersPerAgent.agent;
            WorkerParameters workerParameters = driver.loadWorkerParameters(workerType, agent.getAddressIndex());
            workersPerAgent.registerWorker(workerParameters);

            List<WorkerParameters> workerParametersList = workerDeployment.get(agent.getAddress());
            workerParametersList.add(workerParameters);
        }

        return this;
    }

    private WorkersPerAgent nextAgent(String workerType) {
        WorkersPerAgent smallest = null;
        for (WorkersPerAgent agent : workersPerAgentList) {
            if (!agent.allowed(workerType)) {
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
            throw new CommandLineExitException(format("No suitable agent found for workerType [%s]", workerType));
        }

        return smallest;
    }

    public void printLayout() {
        LOGGER.info(HORIZONTAL_RULER);
        LOGGER.info("Cluster layout");
        LOGGER.info(HORIZONTAL_RULER);

        for (WorkersPerAgent workersPerAgent : workersPerAgentList) {
            Set<String> agentVersionSpecs = workersPerAgent.getVersionSpecs();
            int agentMemberWorkerCount = workersPerAgent.count("member");
            int agentClientWorkerCount = workersPerAgent.workers.size() - agentMemberWorkerCount;
            int totalWorkerCount = agentMemberWorkerCount + agentClientWorkerCount;

            String message = "    Agent %s (%s) members: %s, clients: %s";
            if (totalWorkerCount > 0) {
                message += ", mode: %s, version specs: %s";
            } else {
                message += " (no workers)";
            }
            LOGGER.info(format(message,
                    workersPerAgent.agent.formatIpAddresses(),
                    workersPerAgent.agent.getAddress(),
                    formatLong(agentMemberWorkerCount, 2),
                    formatLong(agentClientWorkerCount, 2),
                    padLeft(workersPerAgent.agent.getAgentWorkerMode().toString(), WORKER_MODE_LENGTH),
                    agentVersionSpecs
            ));
        }
    }

    public Set<String> getVersionSpecs() {
        Set<String> result = new HashSet<>();
        for (WorkersPerAgent workersPerAgent : workersPerAgentList) {
            result.addAll(workersPerAgent.getVersionSpecs());
        }

        return result;
    }

    public Map<SimulatorAddress, List<WorkerParameters>> getWorkerDeployment() {
        return workerDeployment;
    }

    public int getMemberWorkerCount() {
        int result = 0;
        for (WorkersPerAgent workersPerAgent : workersPerAgentList) {
            for (WorkerParameters workerParameters : workersPerAgent.workers) {
                if (workerParameters.getWorkerType().equals("member")) {
                    result++;
                }
            }
        }

        return result;
    }

    public int getClientWorkerCount() {
        int result = 0;
        for (WorkersPerAgent workersPerAgent : workersPerAgentList) {
            for (WorkerParameters workerParameters : workersPerAgent.workers) {
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
    static final class WorkersPerAgent {

        final List<WorkerParameters> workers = new ArrayList<>();
        final AgentData agent;

        WorkersPerAgent(AgentData agent) {
            this.agent = agent;
        }

        Set<String> getVersionSpecs() {
            Set<String> result = new HashSet<>();
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
            SimulatorAddress workerAddress = workerAddress(agent.getAddressIndex(), workerIndex);

            String workerDirName = workerAddress.toString() + '-' + agent.getPublicAddress() + '-' + parameters.getWorkerType();
            parameters.set("WORKER_ADDRESS", workerAddress)
                    .set("WORKER_INDEX", workerIndex)
                    .set("PUBLIC_ADDRESS", agent.getPublicAddress())
                    .set("PRIVATE_ADDRESS", agent.getPrivateAddress())
                    .set("WORKER_DIR_NAME", workerDirName);
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
