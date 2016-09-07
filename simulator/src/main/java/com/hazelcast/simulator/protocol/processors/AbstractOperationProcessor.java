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

import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.core.SimulatorMessage;
import com.hazelcast.simulator.protocol.operation.IntegrationTestOperation;
import com.hazelcast.simulator.protocol.operation.LogOperation;
import com.hazelcast.simulator.protocol.operation.OperationType;
import com.hazelcast.simulator.protocol.operation.OperationTypeCounter;
import com.hazelcast.simulator.protocol.operation.SimulatorOperation;
import com.hazelcast.simulator.worker.Promise;
import org.apache.log4j.Logger;

import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.simulator.protocol.core.ResponseType.SUCCESS;
import static com.hazelcast.simulator.protocol.operation.OperationType.getOperationType;
import static java.lang.String.format;

/**
 * Abstract {@link OperationProcessor} with basic implementations for {@link IntegrationTestOperation}, {@link LogOperation).
 */
abstract class AbstractOperationProcessor implements OperationProcessor {

    private static final Logger LOGGER = Logger.getLogger(AbstractOperationProcessor.class);

    private final AtomicInteger processFailureCount = new AtomicInteger();

    @SuppressWarnings("PMD.AvoidCatchingThrowable")
    @Override
    public final void process(SimulatorMessage msg, SimulatorOperation op, Promise promise) throws Exception {
        OperationType operationType = getOperationType(op);
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(getClass().getSimpleName() + ".process(" + op.getClass().getSimpleName() + ')');
        }
        OperationTypeCounter.received(operationType);
        try {
            switch (operationType) {
                case INTEGRATION_TEST:
                    processIntegrationTest(msg, (IntegrationTestOperation) op, promise);
                    return;
                case LOG:
                    processLog((LogOperation) op, msg.getSource());
                    promise.answer(SUCCESS);
                    break;
                default:
                    processOperation(msg, op, promise);
                    return;
            }
        } catch (Exception e) {
            processFailureCount.incrementAndGet();
            onProcessOperationFailure(e);
            throw e;
        }
    }

    protected void onProcessOperationFailure(Throwable t) {
        LOGGER.fatal(t.getMessage(), t);
    }

    private void processIntegrationTest(SimulatorMessage msg, IntegrationTestOperation op, Promise promise) throws Exception {
        switch (op.getType()) {
            case EQUALS:
                if (!IntegrationTestOperation.TEST_DATA.equals(op.getTestData())) {
                    throw new IllegalStateException("operationData has not the expected value");
                }
                break;
            default:
                processOperation(msg, op, promise);
                return;
        }
        promise.answer(SUCCESS);
    }

    private void processLog(LogOperation operation, SimulatorAddress sourceAddress) {
        LOGGER.log(operation.getLevel(), format("[%s] %s", sourceAddress, operation.getMessage()));
    }

    abstract void processOperation(SimulatorMessage msg, SimulatorOperation op, Promise promise) throws Exception;
}
