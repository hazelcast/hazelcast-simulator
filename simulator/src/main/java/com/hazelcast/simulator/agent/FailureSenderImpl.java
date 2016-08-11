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
package com.hazelcast.simulator.agent;

import com.hazelcast.simulator.agent.workerprocess.WorkerProcess;
import com.hazelcast.simulator.common.FailureType;
import com.hazelcast.simulator.common.TestSuite;
import com.hazelcast.simulator.protocol.connector.AgentConnector;
import com.hazelcast.simulator.protocol.core.Response;
import com.hazelcast.simulator.protocol.core.ResponseType;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.core.SimulatorProtocolException;
import com.hazelcast.simulator.protocol.operation.FailureOperation;
import org.apache.log4j.Logger;

import static com.hazelcast.simulator.common.FailureType.WORKER_FINISHED;
import static java.lang.String.format;

class FailureSenderImpl implements FailureSender {

    private static final Logger LOGGER = Logger.getLogger(FailureSenderImpl.class);

    private final String agentAddress;
    private final AgentConnector agentConnector;

    private volatile TestSuite testSuite;

    private int failureCount;

    FailureSenderImpl(String agentAddress, AgentConnector agentConnector) {
        this.agentAddress = agentAddress;
        this.agentConnector = agentConnector;
    }

    public void setTestSuite(TestSuite testSuite) {
        this.testSuite = testSuite;
    }

    @Override
    public boolean sendFailureOperation(String message, FailureType type, WorkerProcess workerProcess,
                                        String testId, String cause) {
        boolean sentSuccessfully = true;
        boolean isFailure = type != WORKER_FINISHED;
        SimulatorAddress workerAddress = workerProcess.getAddress();
        FailureOperation operation = new FailureOperation(message, type, workerAddress, agentAddress,
                workerProcess.getHazelcastAddress(), workerProcess.getId(), testId, testSuite, cause);

        if (isFailure) {
            LOGGER.error(format("Detected failure on Worker %s (%s): %s", workerProcess.getId(), workerProcess.getAddress(),
                    operation.getLogMessage(++failureCount)));
        } else {
            LOGGER.info(format("Worker %s (%s) finished.", workerProcess.getId(), workerProcess.getAddress()));
        }

        try {
            Response response = agentConnector.write(SimulatorAddress.COORDINATOR, operation);
            ResponseType firstErrorResponseType = response.getFirstErrorResponseType();
            if (firstErrorResponseType != ResponseType.SUCCESS) {
                LOGGER.error(format("Could not send failure to coordinator: %s", firstErrorResponseType));
                sentSuccessfully = false;
            } else if (isFailure) {
                LOGGER.info("Failure successfully sent to Coordinator!");
            }
        } catch (SimulatorProtocolException e) {
            if (!(e.getCause() instanceof InterruptedException)) {
                LOGGER.error(format("Could not send failure to coordinator! %s", operation.getFileMessage()), e);
                sentSuccessfully = false;
            }
        }

        if (type.isWorkerFinishedFailure()) {
            String finishedType = (isFailure) ? "failed" : "finished";
            LOGGER.info(format("Removing %s Worker %s from configuration...", finishedType, workerAddress));
            agentConnector.removeWorker(workerAddress.getWorkerIndex());
        }

        return sentSuccessfully;
    }
}
