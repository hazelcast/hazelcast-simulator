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
package com.hazelcast.simulator.agent;

import com.hazelcast.simulator.agent.workerprocess.WorkerProcess;
import com.hazelcast.simulator.agent.workerprocess.WorkerProcessFailureHandler;
import com.hazelcast.simulator.common.FailureType;
import com.hazelcast.simulator.protocol.connector.AgentConnector;
import com.hazelcast.simulator.protocol.core.Response;
import com.hazelcast.simulator.protocol.core.ResponseFuture;
import com.hazelcast.simulator.protocol.core.ResponseType;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.core.SimulatorProtocolException;
import com.hazelcast.simulator.protocol.operation.FailureOperation;
import org.apache.log4j.Logger;

import static com.hazelcast.simulator.protocol.core.SimulatorAddress.COORDINATOR;
import static java.lang.String.format;

class WorkerProcessFailureHandlerImpl implements WorkerProcessFailureHandler {

    private static final Logger LOGGER = Logger.getLogger(WorkerProcessFailureHandlerImpl.class);

    private final String agentAddress;
    private final AgentConnector agentConnector;

    private int failureCount;

    WorkerProcessFailureHandlerImpl(String agentAddress, AgentConnector agentConnector) {
        this.agentAddress = agentAddress;
        this.agentConnector = agentConnector;
    }

    @Override
    public boolean handle(String message, FailureType type, WorkerProcess workerProcess, String testId, String cause) {
        boolean send = send(message, type, workerProcess, testId, cause);

        if (type.isTerminal()) {
            SimulatorAddress workerAddress = workerProcess.getAddress();
            unblockPendingFutures(workerAddress);
            removeFinishedWorker(workerAddress, type);
        }

        return send;
    }

    private boolean send(String message, FailureType type, WorkerProcess workerProcess, String testId, String cause) {
        SimulatorAddress workerAddress = workerProcess.getAddress();
        String workerId = workerProcess.getId();

        FailureOperation operation = new FailureOperation(message, type, workerAddress, agentAddress,
                workerProcess.getHazelcastAddress(), workerId, testId, cause);

        if (type.isPoisonPill()) {
            LOGGER.info(format("Worker %s (%s) finished.", workerId, workerAddress));
        } else {
            LOGGER.error(format("Detected failure on Worker %s (%s): %s", workerId, workerAddress,
                    operation.getLogMessage(++failureCount)));
        }

        boolean send = true;
        try {
            Response response = agentConnector.invoke(COORDINATOR, operation);
            ResponseType firstErrorResponseType = response.getFirstErrorResponseType();
            if (firstErrorResponseType != ResponseType.SUCCESS) {
                LOGGER.error(format("Could not send failure to coordinator: %s", firstErrorResponseType));
                send = false;
            } else if (!type.isPoisonPill()) {
                LOGGER.info("Failure successfully sent to Coordinator!");
            }
        } catch (SimulatorProtocolException e) {
            if (!(e.getCause() instanceof InterruptedException)) {
                LOGGER.error(format("Could not send failure to coordinator! %s", operation.getFileMessage()), e);
                send = false;
            }
        }
        return send;
    }

    private void unblockPendingFutures(SimulatorAddress workerAddress) {
        for (ResponseFuture future : agentConnector.getFutureMap().values()) {
            future.unblockOnFailure(workerAddress, COORDINATOR, workerAddress.getAddressIndex());
        }
    }

    private void removeFinishedWorker(SimulatorAddress workerAddress, FailureType type) {
        String finishedType = (type.isPoisonPill()) ? "finished" : "failed";
        LOGGER.info(format("Removing %s Worker %s from configuration...", finishedType, workerAddress));
        agentConnector.removeWorker(workerAddress.getWorkerIndex());
    }
}
