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
package com.hazelcast.simulator.coordinator.tasks;

import com.hazelcast.simulator.agent.operations.CreateWorkerOperation;
import com.hazelcast.simulator.agent.operations.StartTimeoutDetectionOperation;
import com.hazelcast.simulator.agent.workerprocess.WorkerParameters;
import com.hazelcast.simulator.coordinator.registry.AgentData;
import com.hazelcast.simulator.coordinator.registry.Registry;
import com.hazelcast.simulator.coordinator.registry.WorkerData;
import com.hazelcast.simulator.protocol.CoordinatorClient;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.utils.CommandLineExitException;
import com.hazelcast.simulator.utils.ThreadSpawner;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import static com.hazelcast.simulator.utils.CommonUtils.getElapsedSeconds;
import static com.hazelcast.simulator.utils.FormatUtils.HORIZONTAL_RULER;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MINUTES;

/**
 * Starts all Simulator Workers.
 * <p>
 * It receives a map with {@link SimulatorAddress} of the Agent to start the Workers on as key.
 * The value is a list of {@link WorkerParameters}, where each item corresponds to a single Worker to create.
 * <p>
 * The Workers will be created in order: First all member Workers are started, then all client Workers.
 * This is done to prevent clients running into a non existing cluster.
 */
public class StartWorkersTask {

    private static final Logger LOGGER = Logger.getLogger(StartWorkersTask.class);

    private final CoordinatorClient client;
    private final Registry registry;
    private final int startupDelayMs;
    private final Map<SimulatorAddress, List<WorkerParameters>> memberDeploymentPlan;
    private final Map<SimulatorAddress, List<WorkerParameters>> clientDeploymentPlan;
    private final Map<String, String> tags;
    private long started;
    private List<WorkerData> result = new LinkedList<>();
    private int workerStartupIndex;

    public StartWorkersTask(
            Map<SimulatorAddress, List<WorkerParameters>> deploymentPlan,
            Map<String, String> workerTags,
            CoordinatorClient client,
            Registry registry,
            int startupDelayMs) {
        this.client = client;
        this.registry = registry;
        this.startupDelayMs = startupDelayMs;
        this.tags = workerTags;
        this.memberDeploymentPlan = filterByWorkerType(true, deploymentPlan);
        this.clientDeploymentPlan = filterByWorkerType(false, deploymentPlan);
    }

    public List<WorkerData> run() throws Exception {
        echoStartWorkers();

        // first create all members
        startWorkers(memberDeploymentPlan);

        // then create all clients
        startWorkers(clientDeploymentPlan);

        client.invokeOnAllAgents(new StartTimeoutDetectionOperation(), MINUTES.toMillis(1));

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

    private void startWorkers(Map<SimulatorAddress, List<WorkerParameters>> deploymentPlan) {
        ThreadSpawner spawner = new ThreadSpawner("createWorkers", true);
        for (Map.Entry<SimulatorAddress, List<WorkerParameters>> entry : deploymentPlan.entrySet()) {

            SimulatorAddress agentAddress = entry.getKey();
            AgentData agent = registry.getAgent(agentAddress);

            for (WorkerParameters workerParameters : entry.getValue()) {
                spawner.spawn(new CreateWorkerOnAgentTask(workerParameters, startupDelayMs * workerStartupIndex, agent));
                workerStartupIndex++;
            }
        }
        spawner.awaitCompletion();
    }

    private static Map<SimulatorAddress, List<WorkerParameters>> filterByWorkerType(
            boolean isFullMember, Map<SimulatorAddress, List<WorkerParameters>> deploymentPlan) {

        Map<SimulatorAddress, List<WorkerParameters>> result = new HashMap<>();

        for (Map.Entry<SimulatorAddress, List<WorkerParameters>> entry : deploymentPlan.entrySet()) {
            List<WorkerParameters> filtered = new LinkedList<>();

            for (WorkerParameters workerParameters : entry.getValue()) {
                if (workerParameters.getWorkerType().equals("member") == isFullMember) {
                    filtered.add(workerParameters);
                }
            }

            if (!filtered.isEmpty()) {
                result.put(entry.getKey(), filtered);
            }
        }
        return result;
    }

    private static int count(Map<SimulatorAddress, List<WorkerParameters>> deploymentPlan) {
        int result = 0;
        for (List<WorkerParameters> settings : deploymentPlan.values()) {
            result += settings.size();
        }
        return result;
    }

    private final class CreateWorkerOnAgentTask implements Runnable {

        private final WorkerParameters workerParameters;
        private final AgentData agent;
        private final int startupDelayMs;

        private CreateWorkerOnAgentTask(WorkerParameters workerParameters, int startupDelaysMs, AgentData agent) {
            this.startupDelayMs = startupDelaysMs;
            this.workerParameters = workerParameters;
            this.agent = agent;
        }

        @Override
        public void run() {
            CreateWorkerOperation operation = new CreateWorkerOperation(workerParameters, startupDelayMs);
            Future<String> f = client.submit(agent.getAddress(), operation);
            String r;
            try {
                r = f.get();
            } catch (Exception e) {
                throw new CommandLineExitException("Failed to create worker", e);
            }

            String workerType = workerParameters.getWorkerType();
            String workerAddress = workerParameters.get("WORKER_ADDRESS");
            if (!"SUCCESS".equals(r)) {
                LOGGER.fatal(format("Could not create %s Worker %s, reason: %s", workerType, workerAddress, r));
                throw new CommandLineExitException("Failed to create workers");
            }

            // the worker will automatically inherit all the tags of the agent it runs on and on top of that it
            // its own tags are added.
            Map<String, String> finalTags = new HashMap<>();
            finalTags.putAll(agent.getTags());
            finalTags.putAll(tags);

            LOGGER.info(format("    Created %s Worker %s", workerType, workerAddress));
            List<WorkerData> createdWorkers = registry.addWorkers(asList(workerParameters), finalTags);
            result.addAll(createdWorkers);
        }
    }
}
