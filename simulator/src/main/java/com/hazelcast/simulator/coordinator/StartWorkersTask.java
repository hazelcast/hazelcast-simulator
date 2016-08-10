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
import com.hazelcast.simulator.cluster.AgentWorkerLayout;
import com.hazelcast.simulator.cluster.ClusterLayout;
import com.hazelcast.simulator.protocol.core.Response;
import com.hazelcast.simulator.protocol.core.ResponseType;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.operation.CreateWorkerOperation;
import com.hazelcast.simulator.protocol.operation.StartTimeoutDetectionOperation;
import com.hazelcast.simulator.protocol.registry.ComponentRegistry;
import com.hazelcast.simulator.protocol.registry.WorkerData;
import com.hazelcast.simulator.utils.CommandLineExitException;
import com.hazelcast.simulator.utils.ThreadSpawner;
import com.hazelcast.simulator.worker.WorkerType;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.simulator.utils.CommonUtils.getElapsedSeconds;
import static com.hazelcast.simulator.utils.FormatUtils.HORIZONTAL_RULER;
import static java.lang.String.format;

public class StartWorkersTask {
    private static final Logger LOGGER = Logger.getLogger(StartWorkersTask.class);

    private final ClusterLayout clusterLayout;
    private final RemoteClient remoteClient;
    private final ComponentRegistry componentRegistry;
    private final Echoer echoer;
    private final int workerVmStartupDelayMs;

    public StartWorkersTask(
            ClusterLayout clusterLayout,
            RemoteClient remoteClient,
            ComponentRegistry componentRegistry,
            int workerVmStartupDelayMs) {
        this.clusterLayout = clusterLayout;
        this.remoteClient = remoteClient;
        this.componentRegistry = componentRegistry;
        this.workerVmStartupDelayMs = workerVmStartupDelayMs;
        this.echoer = new Echoer(remoteClient);
    }

    public void run() {
        long started = System.nanoTime();

        echoer.echo(HORIZONTAL_RULER);
        echoer.echo("Starting Workers...");
        echoer.echo(HORIZONTAL_RULER);

        int totalWorkerCount = clusterLayout.getTotalWorkerCount();
        echoer.echo("Starting %d Workers (%d members, %d clients)...",
                totalWorkerCount,
                clusterLayout.getMemberWorkerCount(),
                clusterLayout.getClientWorkerCount());
        startWorkers(true);

        if (componentRegistry.workerCount() > 0) {
            WorkerData firstWorker = componentRegistry.getFirstWorker();
            echoer.echo("Worker for global test phases will be %s (%s)", firstWorker.getAddress(),
                    firstWorker.getSettings().getWorkerType());
        }

        long elapsed = getElapsedSeconds(started);
        echoer.echo(HORIZONTAL_RULER);
        echoer.echo("Finished starting of %s Worker JVMs (%s seconds)", totalWorkerCount, elapsed);
        echoer.echo(HORIZONTAL_RULER);
    }

    private void startWorkers(boolean startPokeThread) {
        startWorkersByType(true);
        startWorkersByType(false);

        remoteClient.sendToAllAgents(new StartTimeoutDetectionOperation());
        if (startPokeThread) {
            remoteClient.startWorkerPingThread();
        }
    }

    private void startWorkersByType(boolean isMemberType) {
        ThreadSpawner spawner = new ThreadSpawner("createWorkers", true);
        int workerIndex = 0;
        for (AgentWorkerLayout agentWorkerLayout : clusterLayout.getAgentWorkerLayouts()) {
            List<WorkerProcessSettings> workersSettings = makeWorkersProcessSettings(isMemberType, agentWorkerLayout);

            if (workersSettings.isEmpty()) {
                continue;
            }

            SimulatorAddress agentAddress = agentWorkerLayout.getSimulatorAddress();
            String workerType = (isMemberType) ? "member" : "client";

            int startupDelayMs = workerVmStartupDelayMs * workerIndex;
            spawner.spawn(new StartWorkersOnAgentTask(workersSettings, startupDelayMs, agentAddress, workerType));

            if (isMemberType) {
                workerIndex++;
            }
        }
        spawner.awaitCompletion();
    }

    private List<WorkerProcessSettings> makeWorkersProcessSettings(boolean isMemberType, AgentWorkerLayout agentWorkerLayout) {
        List<WorkerProcessSettings> result = new ArrayList<WorkerProcessSettings>();
        for (WorkerProcessSettings workerProcessSettings : agentWorkerLayout.getWorkerProcessSettings()) {
            WorkerType workerType = workerProcessSettings.getWorkerType();
            if (workerType.isMember() == isMemberType) {
                result.add(workerProcessSettings);
            }
        }
        return result;
    }

    private final class StartWorkersOnAgentTask implements Runnable {
        private final List<WorkerProcessSettings> workersSettings;
        private final SimulatorAddress agentAddress;
        private final String workerType;
        private final int startupDelayMs;

        private StartWorkersOnAgentTask(List<WorkerProcessSettings> workersSettings,
                                        int startupDelaysMs,
                                        SimulatorAddress agentAddress,
                                        String workerType) {
            this.startupDelayMs = startupDelaysMs;
            this.workersSettings = workersSettings;
            this.agentAddress = agentAddress;
            this.workerType = workerType;
        }

        @Override
        public void run() {
            CreateWorkerOperation operation = new CreateWorkerOperation(workersSettings, startupDelayMs);
            Response response = remoteClient.getCoordinatorConnector().write(agentAddress, operation);

            ResponseType responseType = response.getFirstErrorResponseType();
            if (responseType != ResponseType.SUCCESS) {
                throw new CommandLineExitException(format("Could not create %d %s Worker on %s (%s)",
                        workersSettings.size(), workerType, agentAddress, responseType));
            }

            LOGGER.info(format("Created %d %s Worker on %s", workersSettings.size(), workerType, agentAddress));
            componentRegistry.addWorkers(agentAddress, workersSettings);
        }
    }
}
