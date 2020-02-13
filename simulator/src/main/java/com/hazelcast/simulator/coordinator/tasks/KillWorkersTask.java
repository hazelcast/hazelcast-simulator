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

import com.hazelcast.simulator.coordinator.registry.Registry;
import com.hazelcast.simulator.coordinator.registry.WorkerData;
import com.hazelcast.simulator.coordinator.registry.WorkerQuery;
import com.hazelcast.simulator.protocol.CoordinatorClient;
import com.hazelcast.simulator.worker.operations.ExecuteScriptOperation;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static com.hazelcast.simulator.coordinator.registry.WorkerData.toAddressString;
import static com.hazelcast.simulator.utils.CommonUtils.currentTimeSeconds;
import static com.hazelcast.simulator.utils.CommonUtils.sleepSeconds;
import static java.lang.Math.min;
import static java.lang.String.format;

public class KillWorkersTask {

    private static final Logger LOGGER = Logger.getLogger(KillWorkersTask.class);

    private static final int CHECK_INTERVAL_SECONDS = 5;

    private final Registry registry;
    private final CoordinatorClient client;
    private final String command;
    private final WorkerQuery workerQuery;
    private final List<WorkerData> result = new ArrayList<>();
    private final int workerShutdownTimeoutSeconds;

    public KillWorkersTask(
            Registry registry,
            CoordinatorClient client,
            String command,
            WorkerQuery workerQuery,
            int workerShutdownTimeoutSeconds) {
        this.registry = registry;
        this.client = client;
        this.command = command;
        this.workerQuery = workerQuery;
        this.workerShutdownTimeoutSeconds = workerShutdownTimeoutSeconds;
    }

    public List<WorkerData> run() throws Exception {
        List<WorkerData> victims = workerQuery.execute(registry.getWorkers());

        if (victims.isEmpty()) {
            LOGGER.info("No victims found");
            return victims;
        }

        LOGGER.info("Killing " + victims.size() + " workers starting");

        killWorkers(victims);

        awaitTermination(victims);

        LOGGER.info("Killing " + victims.size() + " workers complete");

        return result;
    }

    private void killWorkers(List<WorkerData> victims) {
        LOGGER.info(format("Killing [%s]", toAddressString(victims)));

        for (WorkerData victim : victims) {
            victim.setIgnoreFailures(true);

            client.submit(victim.getAddress(), new ExecuteScriptOperation(command, true));

            LOGGER.info("Kill send to worker [" + victim.getAddress() + "]");
        }
    }

    private void awaitTermination(List<WorkerData> victims) {
        Set<WorkerData> aliveVictims = new HashSet<>(victims);

        long deadlineSeconds = currentTimeSeconds() + workerShutdownTimeoutSeconds;

        for (; ; ) {
            Iterator<WorkerData> it = aliveVictims.iterator();
            while (it.hasNext()) {
                WorkerData victim = it.next();
                if (registry.findWorker(victim.getAddress()) == null) {
                    result.add(victim);
                    it.remove();
                }
            }

            if (aliveVictims.isEmpty()) {
                LOGGER.info(format("Killing of workers [%s] success", toAddressString(victims)));
                break;
            }

            long remainingSeconds = deadlineSeconds - currentTimeSeconds();

            if (remainingSeconds <= 0) {
                LOGGER.info(format("Killing of %s workers failed, following failed to terminate [%s]",
                        aliveVictims.size(), toAddressString(aliveVictims)));
                break;
            }

            LOGGER.info(format("Waiting for [%s] to die", toAddressString(aliveVictims)));
            sleepSeconds(min(remainingSeconds, CHECK_INTERVAL_SECONDS));
        }
    }
}
