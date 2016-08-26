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

import com.hazelcast.simulator.protocol.connector.CoordinatorConnector;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.operation.IgnoreWorkerFailureOperation;
import com.hazelcast.simulator.protocol.operation.KillWorkerOperation;
import com.hazelcast.simulator.protocol.registry.ComponentRegistry;
import com.hazelcast.simulator.protocol.registry.WorkerData;
import org.apache.log4j.Logger;

import java.util.Collections;
import java.util.List;

import static com.hazelcast.simulator.utils.CommonUtils.sleepSeconds;

public class KillWorkersTask {
    private static final int MAX_KILL_TIMEOUT_SECONDS = 60;

    private static final Logger LOGGER = Logger.getLogger(KillWorkersTask.class);

    private final ComponentRegistry componentRegistry;
    private final CoordinatorConnector coordinatorConnector;

    public KillWorkersTask(ComponentRegistry componentRegistry, CoordinatorConnector coordinatorConnector) {
        this.componentRegistry = componentRegistry;
        this.coordinatorConnector = coordinatorConnector;
    }

    public void run() throws Exception {
        WorkerData randomMember = getRandomWorker();
        if (randomMember == null) {
            LOGGER.info("Kill worker ignored; no members found");
            return;
        }

        SimulatorAddress memberAddress = randomMember.getAddress();

        componentRegistry.removeWorker(memberAddress);

        coordinatorConnector.write(memberAddress.getParent(), new IgnoreWorkerFailureOperation(memberAddress));

        coordinatorConnector.writeAsync(memberAddress, new KillWorkerOperation());

        LOGGER.info("Kill send to worker [" + memberAddress + "]");

        awaitTermination(memberAddress);

        LOGGER.info("Kill worker [" + memberAddress + "] completed");
    }

    private void awaitTermination(SimulatorAddress memberAddress) {
        for (int k = 0; k < MAX_KILL_TIMEOUT_SECONDS; k++) {
            if (componentRegistry.getWorker(memberAddress) == null) {
                break;
            }
            LOGGER.info("Waiting for worker to terminate " + k + " seconds");
            sleepSeconds(1);
        }
    }

    private WorkerData getRandomWorker() {
        List<WorkerData> workers = componentRegistry.getWorkers();
        Collections.shuffle(workers);

        WorkerData randomMember = null;
        for (WorkerData workerData : workers) {
            if (workerData.isMemberWorker()) {
                randomMember = workerData;
                break;
            }
        }

        return randomMember;
    }
}
