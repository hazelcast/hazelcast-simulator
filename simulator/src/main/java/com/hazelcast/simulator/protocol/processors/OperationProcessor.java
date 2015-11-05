/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.simulator.protocol.operation.LogOperation;
import com.hazelcast.simulator.protocol.operation.OperationType;
import com.hazelcast.simulator.protocol.operation.SimulatorOperation;
import org.apache.log4j.Logger;

import static com.hazelcast.simulator.protocol.core.ResponseType.EXCEPTION_DURING_OPERATION_EXECUTION;
import static com.hazelcast.simulator.protocol.core.ResponseType.SUCCESS;
import static com.hazelcast.simulator.protocol.operation.OperationType.getOperationType;
import static java.lang.String.format;

/**
 * Processes {@link SimulatorOperation} instances on a Simulator component.
 */
public abstract class OperationProcessor {

    private static final Logger LOGGER = Logger.getLogger(OperationProcessor.class);

    private final ExceptionLogger exceptionLogger;

    OperationProcessor(ExceptionLogger exceptionLogger) {
        this.exceptionLogger = exceptionLogger;
    }

    public void shutdown() {
    }

    @SuppressWarnings("PMD.AvoidCatchingThrowable")
    public final ResponseType process(SimulatorOperation operation, SimulatorAddress sourceAddress) {
        OperationType operationType = getOperationType(operation);
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(getClass().getSimpleName() + ".process(" + operation.getClass().getSimpleName() + ')');
        }
        try {
            switch (operationType) {
                case INTEGRATION_TEST:
                    processIntegrationTest((IntegrationTestOperation) operation);
                    break;
                case LOG:
                    processLog((LogOperation) operation, sourceAddress);
                    break;
                default:
                    return processOperation(operationType, operation, sourceAddress);
            }
        } catch (Throwable e) {
            exceptionLogger.log(e);
            return EXCEPTION_DURING_OPERATION_EXECUTION;
        }
        return SUCCESS;
    }

    private void processIntegrationTest(IntegrationTestOperation operation) {
        if (!IntegrationTestOperation.TEST_DATA.equals(operation.getTestData())) {
            throw new IllegalStateException("operationData has not the expected value");
        }
    }

    private void processLog(LogOperation operation, SimulatorAddress sourceAddress) {
        LOGGER.log(operation.getLevel(), format("[%s] %s", sourceAddress, operation.getMessage()));
    }

    protected abstract ResponseType processOperation(OperationType operationType, SimulatorOperation operation,
                                                     SimulatorAddress sourceAddress) throws Exception;
}
