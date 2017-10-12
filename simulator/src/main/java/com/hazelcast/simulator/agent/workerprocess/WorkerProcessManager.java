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
package com.hazelcast.simulator.agent.workerprocess;

import com.hazelcast.simulator.protocol.core.AddressLevel;
import com.hazelcast.simulator.protocol.core.Response;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.utils.ThreadSpawner;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class WorkerProcessManager {

    private static final Logger LOGGER = Logger.getLogger(WorkerProcessManager.class);

    private final ConcurrentMap<SimulatorAddress, WorkerProcess> workerProcesses
            = new ConcurrentHashMap<SimulatorAddress, WorkerProcess>();

    public void add(SimulatorAddress workerAddress, WorkerProcess workerProcess) {
        workerProcesses.put(workerAddress, workerProcess);
    }

    public void remove(WorkerProcess process) {
        workerProcesses.remove(process.getAddress());
    }

    public Collection<WorkerProcess> getWorkerProcesses() {
        return workerProcesses.values();
    }

    public void updateLastSeenTimestamp(Response response) {
        for (Map.Entry<SimulatorAddress, Response.Part> entry : response.getParts()) {
            updateLastSeenTimestamp(entry.getKey());
        }
    }

    public void updateLastSeenTimestamp(SimulatorAddress sourceAddress) {
        AddressLevel sourceAddressLevel = sourceAddress.getAddressLevel();
        if (sourceAddressLevel == AddressLevel.TEST) {
            sourceAddress = sourceAddress.getParent();
        } else if (sourceAddressLevel != AddressLevel.WORKER) {
            LOGGER.warn("Should update LastSeenTimestamp for unsupported AddressLevel: " + sourceAddress);
            return;
        }

        WorkerProcess workerProcess = workerProcesses.get(sourceAddress);
        if (workerProcess == null) {
            LOGGER.warn("Should update LastSeenTimestamp for unknown WorkerJVM: " + sourceAddress);
        } else {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Updated LastSeenTimestamp for: " + sourceAddress);
            }
            workerProcess.updateLastSeen();
        }
    }

    public void shutdown() {
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
}
