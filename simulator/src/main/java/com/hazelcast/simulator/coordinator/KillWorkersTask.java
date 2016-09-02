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
import com.hazelcast.simulator.protocol.operation.ExecuteScriptOperation;
import com.hazelcast.simulator.protocol.registry.ComponentRegistry;
import com.hazelcast.simulator.protocol.registry.WorkerData;
import com.hazelcast.simulator.protocol.registry.WorkerQuery;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static com.hazelcast.simulator.protocol.registry.WorkerData.toAddressString;
import static com.hazelcast.simulator.utils.CommonUtils.sleepSeconds;
import static java.lang.String.format;

public class KillWorkersTask {

    private static final Logger LOGGER = Logger.getLogger(KillWorkersTask.class);

    private static final int WORKER_TERMINATION_TIMEOUT_SECONDS = 300;
    private static final int WORKER_TERMINATION_CHECK_DELAY = 5;

    private final ComponentRegistry componentRegistry;
    private final CoordinatorConnector coordinatorConnector;
    private final String command;
    private final WorkerQuery workerQuery;
    private final List<WorkerData> result = new ArrayList<WorkerData>();

    public KillWorkersTask(
            ComponentRegistry componentRegistry,
            CoordinatorConnector coordinatorConnector,
            String command,
            WorkerQuery workerQuery) {
        this.componentRegistry = componentRegistry;
        this.coordinatorConnector = coordinatorConnector;
        this.command = command;
        this.workerQuery = workerQuery;
    }

    public List<WorkerData> run() throws Exception {
        LOGGER.info("Killing " + workerQuery.getMaxCount() + " workers starting");

        List<WorkerData> victims = workerQuery.execute(componentRegistry.getWorkers());
        if (victims.isEmpty()) {
            LOGGER.info("No victims found");
            return victims;
        }

        killWorkers(victims);

        awaitTermination(victims);

        LOGGER.info("Killing " + workerQuery.getMaxCount() + " workers complete");

        return result;
    }

    private void killWorkers(List<WorkerData> victims) {
        if (victims.size() < workerQuery.getMaxCount()) {
            LOGGER.info(format("Killing %s of the requested %s workers", victims.size(), workerQuery.getMaxCount()));
        }

        LOGGER.info(format("Killing [%s]", toAddressString(victims)));

        for (WorkerData victim : victims) {
            victim.setIgnoreFailures(true);

            coordinatorConnector.invokeAsync(victim.getAddress(), new ExecuteScriptOperation(command));

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
                    result.add(victim);
                    it.remove();
                }
            }

            if (aliveVictims.isEmpty()) {
                break;
            }

            LOGGER.info(format("Waiting for [%s] to die", toAddressString(aliveVictims)));
            sleepSeconds(WORKER_TERMINATION_CHECK_DELAY);
        }

        if (aliveVictims.isEmpty()) {
            LOGGER.info(format("Killing of workers [%s] success", toAddressString(victims)));
        } else {
            LOGGER.info(format("Killing of workers [%s] failed, following failed to terminate [%s]",
                    toAddressString(victims), toAddressString(victims)));
        }
    }
}
