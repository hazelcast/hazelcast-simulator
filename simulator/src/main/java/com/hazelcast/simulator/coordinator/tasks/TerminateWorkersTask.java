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

import com.hazelcast.simulator.agent.operations.StopTimeoutDetectionOperation;
import com.hazelcast.simulator.common.SimulatorProperties;
import com.hazelcast.simulator.coordinator.registry.ComponentRegistry;
import com.hazelcast.simulator.coordinator.registry.WorkerData;
import com.hazelcast.simulator.protocol.CoordinatorClient;
import com.hazelcast.simulator.worker.operations.TerminateWorkerOperation;
import org.apache.log4j.Logger;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static com.hazelcast.simulator.utils.CommonUtils.getElapsedSeconds;
import static com.hazelcast.simulator.utils.CommonUtils.sleepMillis;
import static com.hazelcast.simulator.utils.CommonUtils.sleepSeconds;
import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * This task shuts down all workers.
 */
public class TerminateWorkersTask {

    private static final int FINISHED_WORKERS_SLEEP_MILLIS = 500;

    private static final Logger LOGGER = Logger.getLogger(TerminateWorkersTask.class);
    private final ComponentRegistry registry;
    private final SimulatorProperties simulatorProperties;
    private final CoordinatorClient client;

    public TerminateWorkersTask(
            SimulatorProperties simulatorProperties,
            ComponentRegistry registry,
            CoordinatorClient client) {
        this.simulatorProperties = simulatorProperties;
        this.registry = registry;
        this.client = client;
    }

    public void run() {
        try {
            run0();
        } catch (Exception e) {
            LOGGER.error("Failed to terminate workers", e);
        }
    }

    private void run0() throws TimeoutException, InterruptedException, ExecutionException {
        int currentWorkerCount = registry.workerCount();
        if (currentWorkerCount == 0) {
            return;
        }

        LOGGER.info(format("Terminating %d Workers...", currentWorkerCount));

        client.invokeAll(registry.getAgents(), new StopTimeoutDetectionOperation(), MINUTES.toMillis(1));

        // prevent any failures from being printed due to killing the members.
        Set<WorkerData> clients = new HashSet<WorkerData>();
        Set<WorkerData> members = new HashSet<WorkerData>();
        for (WorkerData worker : registry.getWorkers()) {
            worker.setIgnoreFailures(true);
            if (worker.getParameters().getWorkerType().equals("member")) {
                members.add(worker);
            } else {
                clients.add(worker);
            }
        }

        // first shut down all clients
        for (WorkerData worker : clients) {
            client.send(worker.getAddress(), new TerminateWorkerOperation(true));
        }

        // wait some if there were any clients
        if (!clients.isEmpty()) {
            sleepSeconds(simulatorProperties.getMemberWorkerShutdownDelaySeconds());
        }

        // and then terminate all members
        for (WorkerData worker : members) {
            client.send(worker.getAddress(), new TerminateWorkerOperation(true));
        }

        // now we wait for the workers to die
        waitForWorkerShutdown(currentWorkerCount);
    }

    private void waitForWorkerShutdown(int expectedWorkerCount) {
        int timeoutSeconds = simulatorProperties.getWaitForWorkerShutdownTimeoutSeconds();

        long started = System.nanoTime();
        LOGGER.info(format("Waiting up to %d seconds for shutdown of %d Workers...", timeoutSeconds, expectedWorkerCount));
        long expirationTimeMillis = currentTimeMillis() + SECONDS.toMillis(timeoutSeconds);

        while (registry.workerCount() > 0 && currentTimeMillis() < expirationTimeMillis) {
            sleepMillis(FINISHED_WORKERS_SLEEP_MILLIS);
        }

        List<WorkerData> remainingWorkers = registry.getWorkers();
        if (remainingWorkers.isEmpty()) {
            LOGGER.info(format("Finished shutdown of all Workers (%d seconds)", getElapsedSeconds(started)));
        } else {
            LOGGER.warn(format("Aborted waiting for shutdown of all Workers (%d still running)...", remainingWorkers.size()));
            LOGGER.warn(format("Unfinished workers: %s", toString(remainingWorkers)));
        }
    }

    private String toString(List<WorkerData> remainingWorkers) {
        StringBuilder sb = new StringBuilder();
        for (int k = 0; k < remainingWorkers.size(); k++) {
            sb.append(remainingWorkers.get(k).getAddress());
            if (k < remainingWorkers.size() - 1) {
                sb.append(',');
            }
        }
        return sb.toString();
    }
}
