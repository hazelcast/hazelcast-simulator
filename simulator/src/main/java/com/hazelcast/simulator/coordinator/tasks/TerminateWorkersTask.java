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

import com.hazelcast.simulator.common.SimulatorProperties;
import com.hazelcast.simulator.coordinator.RemoteClient;
import com.hazelcast.simulator.protocol.operation.StopTimeoutDetectionOperation;
import com.hazelcast.simulator.protocol.operation.TerminateWorkerOperation;
import com.hazelcast.simulator.coordinator.registry.ComponentRegistry;
import com.hazelcast.simulator.coordinator.registry.WorkerData;
import org.apache.log4j.Logger;

import java.util.List;

import static com.hazelcast.simulator.utils.CommonUtils.getElapsedSeconds;
import static com.hazelcast.simulator.utils.CommonUtils.sleepMillis;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * This task shuts down all workers.
 */
public class TerminateWorkersTask {

    private static final int FINISHED_WORKERS_SLEEP_MILLIS = 500;

    private static final Logger LOGGER = Logger.getLogger(TerminateWorkersTask.class);
    private final ComponentRegistry componentRegistry;
    private final SimulatorProperties simulatorProperties;
    private final RemoteClient remoteClient;

    public TerminateWorkersTask(
            SimulatorProperties simulatorProperties,
            ComponentRegistry componentRegistry,
            RemoteClient remoteClient) {
        this.simulatorProperties = simulatorProperties;
        this.componentRegistry = componentRegistry;
        this.remoteClient = remoteClient;
    }

    public void run() {
        int currentWorkerCount = componentRegistry.workerCount();

        LOGGER.info(format("Terminating %d Workers...", currentWorkerCount));
        terminateWorkers();

        waitForWorkerShutdown(currentWorkerCount);
    }

    private void terminateWorkers() {
        remoteClient.invokeOnAllAgents(new StopTimeoutDetectionOperation());

        int shutdownDelaySeconds = componentRegistry.hasClientWorkers()
                ? simulatorProperties.getMemberWorkerShutdownDelaySeconds()
                : 0;

        // prevent any failures from being printed due to killing the members.
        for (WorkerData worker : componentRegistry.getWorkers()) {
            worker.setIgnoreFailures(true);
        }

        remoteClient.invokeOnAllWorkers(new TerminateWorkerOperation(shutdownDelaySeconds, true));
    }

    private void waitForWorkerShutdown(int expectedWorkerCount) {
        int timeoutSeconds = simulatorProperties.getWaitForWorkerShutdownTimeoutSeconds();

        long started = System.nanoTime();
        LOGGER.info(format("Waiting up to %d seconds for shutdown of %d Workers...", timeoutSeconds, expectedWorkerCount));
        long expirationTimeMillis = System.currentTimeMillis() + SECONDS.toMillis(timeoutSeconds);

        while (componentRegistry.workerCount() > 0 && System.currentTimeMillis() < expirationTimeMillis) {
            sleepMillis(FINISHED_WORKERS_SLEEP_MILLIS);
        }

        List<WorkerData> remainingWorkers = componentRegistry.getWorkers();
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
