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
import com.hazelcast.simulator.common.WorkerType;
import com.hazelcast.simulator.protocol.connector.CoordinatorConnector;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.operation.KillWorkerOperation;
import com.hazelcast.simulator.protocol.operation.SimulatorOperation;
import com.hazelcast.simulator.protocol.registry.ComponentRegistry;
import com.hazelcast.simulator.protocol.registry.WorkerData;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static com.hazelcast.simulator.utils.CommonUtils.sleepSeconds;
import static java.lang.String.format;

public class KillWorkersTask {

    private static final Logger LOGGER = Logger.getLogger(KillWorkersTask.class);

    private static final int WORKER_TERMINATION_TIMEOUT_SECONDS = 300;
    private static final int WORKER_TERMINATION_CHECK_DELAY = 5;

    private final ComponentRegistry componentRegistry;
    private final CoordinatorConnector coordinatorConnector;
    private final int count;
    private final String versionSpec;
    private final WorkerType workerType;
    private final String agentAddress;
    private final String workerAddress;

    public KillWorkersTask(
            ComponentRegistry componentRegistry,
            CoordinatorConnector coordinatorConnector,
            int count,
            String versionSpec,
            WorkerType workerType,
            String agentAddress,
            String workerAddress) {
        this.componentRegistry = componentRegistry;
        this.coordinatorConnector = coordinatorConnector;
        this.count = count;
        this.versionSpec = versionSpec;
        this.workerType = workerType;
        this.agentAddress = agentAddress;
        this.workerAddress = workerAddress;
    }

    public void run() throws Exception {
        LOGGER.info("Killing " + count + " workers starting");

        List<WorkerData> victims = getVictims();
        if (victims.isEmpty()) {
            LOGGER.info("No victims found");
            return;
        }

        killWorkers(victims);

        awaitTermination(victims);

        LOGGER.info("Killing " + count + " workers complete");
    }

    private List<WorkerData> getVictims() {
        List<WorkerData> workers = componentRegistry.getWorkers();
        Collections.shuffle(workers);

        List<WorkerData> victims = new ArrayList<WorkerData>();
        for (WorkerData worker : workers) {
            if (victims.size() == count) {
                break;
            }

            if (isVictim(worker)) {
                victims.add(worker);
            }
        }

        return victims;
    }

    @SuppressWarnings("checkstyle:npathcomplexity")
    private boolean isVictim(WorkerData workerData) {
        WorkerProcessSettings workerProcessSettings = workerData.getSettings();

        if (versionSpec != null) {
            if (!workerProcessSettings.getVersionSpec().equals(versionSpec)) {
                return false;
            }
        }

        if (workerAddress != null) {
            if (!workerData.getAddress().equals(SimulatorAddress.fromString(workerAddress))) {
                return false;
            }
        }

        if (agentAddress != null) {
            if (!workerData.getAddress().getParent().equals(SimulatorAddress.fromString(agentAddress))) {
                return false;
            }
        }

        if (!workerProcessSettings.getWorkerType().equals(workerType)) {
            return false;
        }

        return true;
    }

    private void killWorkers(List<WorkerData> victims) {
        if (victims.size() < count) {
            LOGGER.info(format("Killing %s of the requested %s workers", victims.size(), count));
        }

        LOGGER.info(format("Killing [%s]", toString(victims)));

        for (WorkerData victim : victims) {
            victim.setIgnoreFailures(true);

            SimulatorOperation killOperation = new KillWorkerOperation(); //BashOperation("kill $PID");
            coordinatorConnector.writeAsync(victim.getAddress(), killOperation);

            LOGGER.info("Kill send to worker [" + victim.getAddress() + "]");
        }
    }

    private void awaitTermination(List<WorkerData> victims) {
        Set<WorkerData> aliveVictims = new HashSet<WorkerData>(victims);

        for (int k = 0; k < WORKER_TERMINATION_TIMEOUT_SECONDS / WORKER_TERMINATION_CHECK_DELAY; k++) {
            Iterator<WorkerData> it = aliveVictims.iterator();
            while (it.hasNext()) {
                WorkerData victim = it.next();
                if (componentRegistry.findWorker(victim.getAddress()) == null) {
                    it.remove();
                }
            }

            if (aliveVictims.isEmpty()) {
                break;
            }

            LOGGER.info(format("Waiting for [%s] to die", toString(aliveVictims)));
            sleepSeconds(WORKER_TERMINATION_CHECK_DELAY);
        }

        if (aliveVictims.isEmpty()) {
            LOGGER.info(format("Killing of workers [%s] success", toString(victims)));
        } else {
            LOGGER.info(format("Killing of workers [%s] failed, following failed to terminate [%s]",
                    toString(victims), toString(victims)));
        }
    }

    private static String toString(Collection<WorkerData> workers) {
        StringBuilder sb = new StringBuilder();

        boolean first = true;
        for (WorkerData worker : workers) {
            if (first) {
                first = false;
            } else {
                sb.append(",");
            }
            sb.append(worker.getAddress());
        }

        return sb.toString();
    }
}
