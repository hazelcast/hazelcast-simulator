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
package com.hazelcast.simulator.agent.workerprocess;

import com.hazelcast.simulator.agent.operations.CreateWorkerOperation;
import com.hazelcast.simulator.coordinator.operations.FailureOperation;
import com.hazelcast.simulator.protocol.Promise;
import com.hazelcast.simulator.protocol.Server;
import com.hazelcast.simulator.protocol.core.AddressLevel;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.operation.LogOperation;
import com.hazelcast.simulator.utils.ThreadSpawner;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.simulator.common.FailureType.WORKER_CREATE_ERROR;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.log4j.Level.DEBUG;

public class WorkerProcessManager {

    private static final Logger LOGGER = Logger.getLogger(WorkerProcessManager.class);

    private final ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();

    private final ConcurrentMap<SimulatorAddress, WorkerProcess> workerProcesses
            = new ConcurrentHashMap<SimulatorAddress, WorkerProcess>();
    private final Server server;
    private final SimulatorAddress agentAddress;
    private final String publicAddress;

    public WorkerProcessManager(Server server, SimulatorAddress agentAddress, String publicAddress) {
        this.server = server;
        this.agentAddress = agentAddress;
        this.publicAddress = publicAddress;
    }

    public String getPublicAddress() {
        return publicAddress;
    }

    public SimulatorAddress getAgentAddress() {
        return agentAddress;
    }

    // launching is done asynchronous so we don't block the calling thread (messaging thread)
    public void launch(CreateWorkerOperation op, Promise promise) throws Exception {
        AtomicInteger remaining = new AtomicInteger(op.getWorkerParametersList().size());
        for (WorkerParameters workerParameters : op.getWorkerParametersList()) {
            WorkerProcessLauncher launcher = new WorkerProcessLauncher(WorkerProcessManager.this, workerParameters);
            LaunchSingleWorkerTask task = new LaunchSingleWorkerTask(launcher, workerParameters, promise, remaining);
            executorService.schedule(task, op.getDelayMs(), MILLISECONDS);
        }
    }

    public void add(SimulatorAddress workerAddress, WorkerProcess workerProcess) {
        workerProcesses.put(workerAddress, workerProcess);
    }

    public void remove(WorkerProcess process) {
        workerProcesses.remove(process.getAddress());
    }

    public Collection<WorkerProcess> getWorkerProcesses() {
        return workerProcesses.values();
    }

    public void updateLastSeenTimestamp(SimulatorAddress workerAddress) {
        WorkerProcess workerProcess = workerProcesses.get(workerAddress);
        if (workerProcess == null) {
            LOGGER.warn("update LastSeenTimestamp for unknown WorkerJVM: " + workerAddress);
            return;
        }

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Updated LastSeenTimestamp for: " + workerAddress);
        }
        workerProcess.updateLastSeen();
    }

    public void shutdown() {
        executorService.shutdown();
        ThreadSpawner spawner = new ThreadSpawner("workerJvmManagerShutdown", true);
        for (final WorkerProcess workerProcess : new ArrayList<WorkerProcess>(workerProcesses.values())) {
            spawner.spawn(new Runnable() {
                @Override
                public void run() {
                    shutdown(workerProcess);
                }
            });
        }
        spawner.awaitCompletion();
    }

    void shutdown(WorkerProcess workerProcess) {
        workerProcesses.remove(workerProcess.getAddress());
        try {
            // this sends SIGTERM on *nix
            Process process = workerProcess.getProcess();
            if (process == null) {
                LOGGER.error("TODO: Embedded worker not SHUTDOWN!!!");
            } else {
                process.destroy();
                process.waitFor();
            }
        } catch (Exception e) {
            LOGGER.error("Failed to destroy Worker process: " + workerProcess, e);
        }
    }

    final class LaunchSingleWorkerTask implements Runnable {

        private final WorkerProcessLauncher launcher;
        private final WorkerParameters parameters;
        private final AtomicInteger remaining;
        private final Promise promise;

        private LaunchSingleWorkerTask(WorkerProcessLauncher launcher,
                                       WorkerParameters parameters,
                                       Promise promise,
                                       AtomicInteger remaining) {
            this.launcher = launcher;
            this.parameters = parameters;
            this.remaining = remaining;
            this.promise = promise;
        }

        @Override
        public void run() {
            try {
                launch();

                if (remaining.decrementAndGet() == 0) {
                    // it was the last worker needing to be created; so lets answer the promise.
                    promise.answer("SUCCESS");
                }
            } catch (Exception e) {
                LOGGER.error("Failed to start Worker:" + workerProcesses, e);

                SimulatorAddress workerAddress
                        = new SimulatorAddress(AddressLevel.WORKER, agentAddress.getAddressIndex(),
                        parameters.intGet("WORKER_INDEX"));

                server.sendCoordinator(new FailureOperation("Failed to start worker [" + workerAddress + "]",
                        WORKER_CREATE_ERROR, workerAddress, agentAddress.toString(), e));

                promise.answer(e.getMessage());
            } catch (Throwable t) {
                // just try to log; anything else than exception no point in dealing with promises
                LOGGER.error("Failed to start Worker", t);
            }
        }

        private void launch() throws Exception {
            launcher.launch();

            int workerIndex = parameters.intGet("WORKER_INDEX");

            String workerType = parameters.getWorkerType();
            SimulatorAddress workerAddress = new SimulatorAddress(
                    AddressLevel.WORKER, agentAddress.getAgentIndex(), workerIndex);

            LogOperation logOperation = new LogOperation(
                    format("Created %s Worker %s", workerType, workerAddress), DEBUG);

            server.sendCoordinator(logOperation);
        }
    }
}
