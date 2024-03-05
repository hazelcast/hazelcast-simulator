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
import com.hazelcast.simulator.utils.FileUtils;
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
    private final Map<String, String> properties = new HashMap<>();

    public DeploymentPlan(Registry registry) {
        this(registry.getAgents());
    }

    public void addProperty(String key, String value) {
        properties.put(key, value);
    }

    public void addAllProperty(Map<String, String> map) {
        properties.putAll(map);
    }

    public DeploymentPlan(List<AgentData> agents) {

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
            WorkerParameters workerParameters = new WorkerParameters();
            workerParameters.setAll(properties);
            workerParameters.set("WORKER_TYPE", workerType);

            workerParameters.set("VERSION_SPEC", workerParameters.get("version"));

            workerParameters.set("file:log4j.xml", "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                    "<!DOCTYPE log4j:configuration SYSTEM \"log4j.dtd\" >\n" +
                    "<log4j:configuration>\n" +
                    "\n" +
                    "    <appender name=\"console\" class=\"org.apache.log4j.ConsoleAppender\">\n" +
                    "        <param name=\"Threshold\" value=\"TRACE\"/>\n" +
                    "        <layout class=\"org.apache.log4j.PatternLayout\">\n" +
                    "            <param name=\"ConversionPattern\" value=\"%-5p %d{HH:mm:ss} %m%n\"/>\n" +
                    "        </layout>\n" +
                    "    </appender>\n" +
                    "\n" +
                    "    <root>\n" +
                    "        <priority value=\"info\"/>\n" +
                    "        <appender-ref ref=\"console\"/>\n" +
                    "    </root>\n" +
                    "</log4j:configuration>\n");

            workerParameters.set("file:worker.sh","#!/bin/bash\n" +
                    "\n" +
                    "#\n" +
                    "# Script to start up a Simulator Worker.\n" +
                    "#\n" +
                    "# To customize the behavior of the Worker, including Java configuration, copy this file into the 'work dir' of Simulator.\n" +
                    "# See the end of this file for examples for different profilers.\n" +
                    "#\n" +
                    "\n" +
                    "# automatic exit on script failure\n" +
                    "set -e\n" +
                    "# printing the command being executed (useful for debugging)\n" +
                    "#set -x\n" +
                    "\n" +
                    "# redirecting output/error to the right log files\n" +
                    "exec > worker.out\n" +
                    "exec 2> worker.err\n" +
                    "\n" +
                    "JVM_ARGS=\"-Dlog4j2.configurationFile=log4j.xml\"\n" +
                    "\n" +
                    "# Include the member/client-worker jvm options\n" +
                    "JVM_ARGS=\"$JVM_OPTIONS $JVM_ARGS\"\n" +
                    "\n" +
                    "MAIN=com.hazelcast.simulator.worker.Worker\n" +
                    "\n" +
                    "java -classpath \"$CLASSPATH\" ${JVM_ARGS} ${MAIN}\n" +
                    "\n"
                   );

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
