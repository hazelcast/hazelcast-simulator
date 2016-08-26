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
import com.hazelcast.simulator.protocol.operation.IgnoreWorkerFailureOperation;
import com.hazelcast.simulator.protocol.operation.KillWorkerOperation;
import com.hazelcast.simulator.protocol.registry.ComponentRegistry;
import com.hazelcast.simulator.protocol.registry.WorkerData;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.hazelcast.simulator.utils.CommonUtils.sleepSeconds;
import static java.lang.String.format;

public class KillWorkersTask {

    private static final Logger LOGGER = Logger.getLogger(KillWorkersTask.class);
    private static final int WORKERS_DIED_TIMEOUT = 10;

    private final ComponentRegistry componentRegistry;
    private final CoordinatorConnector coordinatorConnector;
    private final int count;
    private final String versionSpec;
    private final WorkerType workerType;

    public KillWorkersTask(
            ComponentRegistry componentRegistry,
            CoordinatorConnector coordinatorConnector,
            int count,
            String versionSpec,
            WorkerType workerType) {
        this.componentRegistry = componentRegistry;
        this.coordinatorConnector = coordinatorConnector;
        this.count = count;
        this.versionSpec = versionSpec;
        this.workerType = workerType;
    }

    public void run() throws Exception {
        LOGGER.info("Killing " + count + " workers starting");

        List<WorkerData> victims = getVictims();
        if (victims.isEmpty()) {
            LOGGER.info("No victims found");
            return;
        }

        if (victims.size() < count) {
            LOGGER.info(format("Killing %s of the requested %s workers", victims.size(), count));
        }

        for (WorkerData victim : victims) {
            SimulatorAddress memberAddress = victim.getAddress();

            componentRegistry.removeWorker(memberAddress);

            coordinatorConnector.write(memberAddress.getParent(), new IgnoreWorkerFailureOperation(memberAddress));

            coordinatorConnector.writeAsync(memberAddress, new KillWorkerOperation());

            LOGGER.info("Kill send to worker [" + memberAddress + "]");
        }

        LOGGER.info("Giving workers time to die...");
        sleepSeconds(WORKERS_DIED_TIMEOUT);
        LOGGER.info("Killing " + count + " workers complete");
    }

    private List<WorkerData> getVictims() {
        List<WorkerData> workers = componentRegistry.getWorkers();
        Collections.shuffle(workers);

        List<WorkerData> victims = new ArrayList<WorkerData>();
        for (WorkerData workerData : workers) {
            if (victims.size() == count) {
                break;
            }

            if (isVictim(workerData)) {
                victims.add(workerData);
            }
        }

        return victims;
    }

    private boolean isVictim(WorkerData workerData) {
        WorkerProcessSettings workerProcessSettings = workerData.getSettings();

        if (versionSpec != null) {
            if (!workerProcessSettings.getVersionSpec().equals(versionSpec)) {
                return false;
            }
        }

        if (!workerProcessSettings.getWorkerType().equals(workerType)) {
            return false;
        }

        return true;
    }
}
