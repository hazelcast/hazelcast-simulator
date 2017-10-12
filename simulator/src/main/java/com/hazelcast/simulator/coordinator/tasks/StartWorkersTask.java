/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.simulator.coordinator.tasks;

import com.hazelcast.simulator.agent.workerprocess.WorkerProcessSettings;
import com.hazelcast.simulator.coordinator.RemoteClient;
import com.hazelcast.simulator.protocol.core.Response;
import com.hazelcast.simulator.protocol.core.ResponseType;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.operation.CreateWorkerOperation;
import com.hazelcast.simulator.protocol.operation.StartTimeoutDetectionOperation;
import com.hazelcast.simulator.protocol.registry.AgentData;
import com.hazelcast.simulator.protocol.registry.ComponentRegistry;
import com.hazelcast.simulator.protocol.registry.WorkerData;
import com.hazelcast.simulator.utils.CommandLineExitException;
import com.hazelcast.simulator.utils.ThreadSpawner;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static com.hazelcast.simulator.protocol.core.ResponseType.SUCCESS;
import static com.hazelcast.simulator.utils.CommonUtils.getElapsedSeconds;
import static com.hazelcast.simulator.utils.FormatUtils.HORIZONTAL_RULER;
import static java.lang.String.format;

/**
 * Starts all Simulator Workers.
 * <p>
 * It receives a map with {@link SimulatorAddress} of the Agent to start the Workers on as key.
 * The value is a list of {@link WorkerProcessSettings}, where each item corresponds to a single Worker to create.
 * <p>
 * The Workers will be created in order: First all member Workers are started, then all client Workers.
 * This is done to prevent clients running into a non existing cluster.
 */
public class StartWorkersTask {

    private static final Logger LOGGER = Logger.getLogger(StartWorkersTask.class);

    private final RemoteClient remoteClient;
    private final ComponentRegistry componentRegistry;
    private final int startupDelayMs;
    private final Map<SimulatorAddress, List<WorkerProcessSettings>> memberDeploymentPlan;
    private final Map<SimulatorAddress, List<WorkerProcessSettings>> clientDeploymentPlan;
    private final Map<String, String> tags;

    private long started;
    private List<WorkerData> result = new LinkedList<WorkerData>();

    public StartWorkersTask(
            Map<SimulatorAddress, List<WorkerProcessSettings>> deploymentPlan,
            Map<String, String> workerTags,
            RemoteClient remoteClient,
            ComponentRegistry componentRegistry,
            int startupDelayMs) {
        this.remoteClient = remoteClient;
        this.componentRegistry = componentRegistry;
        this.startupDelayMs = startupDelayMs;
        this.tags = workerTags;
        this.memberDeploymentPlan = filterByWorkerType(true, deploymentPlan);
        this.clientDeploymentPlan = filterByWorkerType(false, deploymentPlan);
    }

    public List<WorkerData> run() {
        echoStartWorkers();

        // first create all members
        startWorkers(true, memberDeploymentPlan);

        // then create all clients
        startWorkers(false, clientDeploymentPlan);

        remoteClient.invokeOnAllAgents(new StartTimeoutDetectionOperation());

        if (componentRegistry.workerCount() > 0) {
            WorkerData firstWorker = componentRegistry.getFirstWorker();
            LOGGER.info(format("Worker for global test phases will be %s (%s)",
                    firstWorker.getAddress(), firstWorker.getSettings().getWorkerType()));
        }

        echoStartComplete();
        return result;
    }

    private void echoStartWorkers() {
        started = System.nanoTime();
        LOGGER.info(HORIZONTAL_RULER);
        LOGGER.info("Starting Workers...");
        LOGGER.info(HORIZONTAL_RULER);

        LOGGER.info(format("Starting %d Workers (%d members, %d clients)...",
                count(memberDeploymentPlan) + count(clientDeploymentPlan),
                count(memberDeploymentPlan), count(clientDeploymentPlan)));
    }

    private void echoStartComplete() {
        long elapsedSeconds = getElapsedSeconds(started);
        LOGGER.info(HORIZONTAL_RULER);
        LOGGER.info(format("Finished starting of %s Worker JVMs (%s seconds)",
                count(memberDeploymentPlan) + count(clientDeploymentPlan), elapsedSeconds));
        LOGGER.info(HORIZONTAL_RULER);
    }

    private void startWorkers(boolean isMember, Map<SimulatorAddress, List<WorkerProcessSettings>> deploymentPlan) {
        ThreadSpawner spawner = new ThreadSpawner("createWorkers", true);
        int workerIndex = 0;
        String workerType = isMember ? "member" : "client";
        for (Map.Entry<SimulatorAddress, List<WorkerProcessSettings>> entry : deploymentPlan.entrySet()) {
            SimulatorAddress agentAddress = entry.getKey();
            AgentData agent = componentRegistry.getAgent(agentAddress);
            List<WorkerProcessSettings> workersSettings = entry.getValue();

            spawner.spawn(new StartWorkersOnAgentTask(workersSettings, startupDelayMs * workerIndex, agent, workerType));

            if (isMember) {
                workerIndex++;
            }
        }
        spawner.awaitCompletion();
    }

    private static Map<SimulatorAddress, List<WorkerProcessSettings>> filterByWorkerType(
            boolean isFullMember, Map<SimulatorAddress, List<WorkerProcessSettings>> deploymentPlan) {

        Map<SimulatorAddress, List<WorkerProcessSettings>> result = new HashMap<SimulatorAddress, List<WorkerProcessSettings>>();

        for (Map.Entry<SimulatorAddress, List<WorkerProcessSettings>> entry : deploymentPlan.entrySet()) {
            List<WorkerProcessSettings> filtered = new LinkedList<WorkerProcessSettings>();

            for (WorkerProcessSettings settings : entry.getValue()) {
                if (settings.getWorkerType().isMember() == isFullMember) {
                    filtered.add(settings);
                }
            }

            if (!filtered.isEmpty()) {
                result.put(entry.getKey(), filtered);
            }
        }
        return result;
    }

    private static int count(Map<SimulatorAddress, List<WorkerProcessSettings>> deploymentPlan) {
        int result = 0;
        for (List<WorkerProcessSettings> settings : deploymentPlan.values()) {
            result += settings.size();
        }
        return result;
    }

    private final class StartWorkersOnAgentTask implements Runnable {

        private final List<WorkerProcessSettings> workersSettings;
        private final AgentData agent;
        private final String workerType;
        private final int startupDelayMs;

        private StartWorkersOnAgentTask(List<WorkerProcessSettings> workersSettings,
                                        int startupDelaysMs,
                                        AgentData agent,
                                        String workerType) {
            this.startupDelayMs = startupDelaysMs;
            this.workersSettings = workersSettings;
            this.agent = agent;
            this.workerType = workerType;
        }

        @Override
        public void run() {
            CreateWorkerOperation operation = new CreateWorkerOperation(workersSettings, startupDelayMs);
            Response response = remoteClient.getConnector().invoke(agent.getAddress(), operation);

            ResponseType responseType = response.getFirstErrorResponseType();
            if (responseType != SUCCESS) {
                throw new CommandLineExitException(format("Could not create %d %s Worker on %s (%s)",
                        workersSettings.size(), workerType, agent.getAddress(), responseType));
            }

            // the worker will automatically inherit all the tags of the agent it runs on and on top of that it
            // its own tags are added.
            Map<String, String> finalTags = new HashMap<String, String>();
            finalTags.putAll(agent.getTags());
            finalTags.putAll(tags);

            LOGGER.info(format("Created %d %s Worker on %s", workersSettings.size(), workerType, agent.getAddress()));
            List<WorkerData> createdWorkers = componentRegistry.addWorkers(agent.getAddress(), workersSettings, finalTags);
            result.addAll(createdWorkers);
        }
    }
}
