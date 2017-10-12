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
package com.hazelcast.simulator.protocol.processors;

import com.hazelcast.simulator.protocol.core.SimulatorAddress;
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
 * Abstract {@link OperationProcessor} with basic implementations for {@link IntegrationTestOperation}, {@link LogOperation}.
 */
abstract class AbstractOperationProcessor implements OperationProcessor {

    private static final Logger LOGGER = Logger.getLogger(AbstractOperationProcessor.class);

    private final AtomicInteger processFailureCount = new AtomicInteger();

    @SuppressWarnings("PMD.AvoidCatchingThrowable")
    @Override
    public final void process(SimulatorOperation op, SimulatorAddress sourceAddress, Promise promise) throws Exception {
        OperationType operationType = getOperationType(op);
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(getClass().getSimpleName() + ".process(" + op.getClass().getSimpleName() + ')');
        }
        OperationTypeCounter.received(operationType);
        try {
            switch (operationType) {
                case INTEGRATION_TEST:
                    processIntegrationTest(operationType, (IntegrationTestOperation) op, sourceAddress, promise);
                    return;
                case LOG:
                    processLog((LogOperation) op, sourceAddress);
                    promise.answer(SUCCESS);
                    break;
                default:
                    processOperation(operationType, op, sourceAddress, promise);
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

    private void processIntegrationTest(OperationType operationType, IntegrationTestOperation op,
                                        SimulatorAddress sourceAddress, Promise promise) throws Exception {
        switch (op.getType()) {
            case EQUALS:
                if (!IntegrationTestOperation.TEST_DATA.equals(op.getTestData())) {
                    throw new IllegalStateException("operationData has not the expected value");
                }
                break;
            default:
                processOperation(operationType, op, sourceAddress, promise);
                return;
        }
        promise.answer(SUCCESS);
    }

    private void processLog(LogOperation op, SimulatorAddress sourceAddress) {
        LOGGER.log(op.getLevel(), format("[%s] %s", sourceAddress, op.getMessage()));
    }

    abstract void processOperation(OperationType operationType, SimulatorOperation op,
                                   SimulatorAddress sourceAddress, Promise promise) throws Exception;
}
