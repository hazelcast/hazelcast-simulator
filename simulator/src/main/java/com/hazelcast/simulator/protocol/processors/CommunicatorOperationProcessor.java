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
package com.hazelcast.simulator.protocol.processors;

import com.hazelcast.simulator.protocol.core.ResponseType;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.exception.ExceptionLogger;
import com.hazelcast.simulator.protocol.operation.IntegrationTestOperation;
import com.hazelcast.simulator.protocol.operation.OperationType;
import com.hazelcast.simulator.protocol.operation.SimulatorOperation;
import org.apache.log4j.Logger;

import static com.hazelcast.simulator.protocol.core.ResponseType.EXCEPTION_DURING_OPERATION_EXECUTION;
import static com.hazelcast.simulator.protocol.core.ResponseType.SUCCESS;
import static com.hazelcast.simulator.protocol.core.ResponseType.UNSUPPORTED_OPERATION_ON_THIS_PROCESSOR;
import static com.hazelcast.simulator.protocol.operation.OperationType.getOperationType;

/**
 * An {@link OperationProcessor} implementation to process {@link SimulatorOperation} instances on a Simulator Communicator.
 */
public class CommunicatorOperationProcessor implements OperationProcessor {

    private static final Logger LOGGER = Logger.getLogger(CommunicatorOperationProcessor.class);

    private final ExceptionLogger exceptionLogger;

    public CommunicatorOperationProcessor(ExceptionLogger exceptionLogger) {
        this.exceptionLogger = exceptionLogger;
    }

    @Override
    public final ResponseType process(SimulatorOperation operation, SimulatorAddress sourceAddress) {
        OperationType operationType = getOperationType(operation);
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(getClass().getSimpleName() + ".process(" + operation.getClass().getSimpleName() + ')');
        }
        try {
            switch (operationType) {
                case INTEGRATION_TEST:
                    return processIntegrationTest((IntegrationTestOperation) operation);
                default:
                    return processOperation();
            }
        } catch (Exception e) {
            exceptionLogger.log(e);
            return EXCEPTION_DURING_OPERATION_EXECUTION;
        }
    }

    private ResponseType processIntegrationTest(IntegrationTestOperation operation) throws Exception {
        switch (operation.getType()) {
            case EQUALS:
                if (!IntegrationTestOperation.TEST_DATA.equals(operation.getTestData())) {
                    throw new IllegalStateException("operationData has not the expected value");
                }
                break;
            default:
                return UNSUPPORTED_OPERATION_ON_THIS_PROCESSOR;
        }
        return SUCCESS;
    }

    private ResponseType processOperation() throws Exception {
        return UNSUPPORTED_OPERATION_ON_THIS_PROCESSOR;
    }
}
