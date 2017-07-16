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

import com.hazelcast.simulator.common.FailureType;
import com.hazelcast.simulator.coordinator.operations.FailureOperation;
import com.hazelcast.simulator.protocol.Server;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import org.apache.log4j.Logger;

import static java.lang.String.format;

public class WorkerProcessFailureHandler {

    private static final Logger LOGGER = Logger.getLogger(WorkerProcessFailureHandler.class);

    private final String agentAddress;
    private final Server server;
    private int failureCount;

    public WorkerProcessFailureHandler(String agentAddress, Server server) {
        this.agentAddress = agentAddress;
        this.server = server;
    }

    public void handle(String message, FailureType type, WorkerProcess workerProcess, String testId, String cause) {
        SimulatorAddress workerAddress = workerProcess.getAddress();
        String workerId = workerProcess.getId();

        FailureOperation failure = new FailureOperation(
                message,
                type,
                workerAddress,
                agentAddress,
                workerId,
                testId,
                cause);

        if (type.isPoisonPill()) {
            LOGGER.info(format("Worker %s (%s) finished.", workerId, workerAddress));
        } else {
            LOGGER.error(format("Detected failure on Worker %s (%s): %s", workerId, workerAddress,
                    failure.getLogMessage(++failureCount)));
        }

        server.sendCoordinator(failure);
    }
}
