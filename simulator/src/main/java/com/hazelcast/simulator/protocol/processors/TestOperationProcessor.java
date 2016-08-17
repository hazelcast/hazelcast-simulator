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

import com.hazelcast.simulator.protocol.core.Response;
import com.hazelcast.simulator.protocol.core.ResponseFuture;
import com.hazelcast.simulator.protocol.core.ResponseType;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.operation.IntegrationTestOperation;
import com.hazelcast.simulator.protocol.operation.LogOperation;
import com.hazelcast.simulator.protocol.operation.OperationType;
import com.hazelcast.simulator.protocol.operation.PhaseCompletedOperation;
import com.hazelcast.simulator.protocol.operation.SimulatorOperation;
import com.hazelcast.simulator.protocol.operation.StartTestOperation;
import com.hazelcast.simulator.protocol.operation.StartTestPhaseOperation;
import com.hazelcast.simulator.protocol.registry.TargetType;
import com.hazelcast.simulator.testcontainer.TestContainer;
import com.hazelcast.simulator.testcontainer.TestPhase;
import com.hazelcast.simulator.utils.ExceptionReporter;
import com.hazelcast.simulator.worker.Worker;
import com.hazelcast.simulator.worker.WorkerType;
import org.apache.log4j.Logger;

import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.simulator.protocol.core.ResponseType.SUCCESS;
import static com.hazelcast.simulator.protocol.core.ResponseType.UNSUPPORTED_OPERATION_ON_THIS_PROCESSOR;
import static com.hazelcast.simulator.protocol.core.SimulatorAddress.COORDINATOR;
import static com.hazelcast.simulator.testcontainer.TestPhase.getLastTestPhase;
import static java.lang.String.format;

/**
 * An {@link OperationProcessor} implementation to process {@link SimulatorOperation} instances on a Simulator Test.
 */
public class TestOperationProcessor extends AbstractOperationProcessor {

    private static final String DASHES = "---------------------------";
    private static final Logger LOGGER = Logger.getLogger(TestOperationProcessor.class);

    private static final AtomicInteger TESTS_PENDING = new AtomicInteger(0);

    private final Worker worker;
    private final WorkerType type;

    private final String testId;
    private final TestContainer testContainer;
    private final SimulatorAddress testAddress;

    public TestOperationProcessor(Worker worker, WorkerType type, TestContainer testContainer,
                                  SimulatorAddress testAddress) {
        this.worker = worker;
        this.type = type;
        this.testId = testContainer.getTestContext().getTestId();
        this.testContainer = testContainer;
        this.testAddress = testAddress;

        TESTS_PENDING.incrementAndGet();
    }

    // just for testing
    static void resetPendingTests() {
        TESTS_PENDING.set(0);
    }

    // just for testing
    TestPhase getTestPhase() {
        return testContainer.currentTestPhase();
    }

    public SimulatorAddress getTestAddress() {
        return testAddress;
    }

    @Override
    protected ResponseType processOperation(OperationType operationType, SimulatorOperation operation,
                                            SimulatorAddress sourceAddress) throws Exception {
        switch (operationType) {
            case INTEGRATION_TEST:
                return processIntegrationTest((IntegrationTestOperation) operation, sourceAddress);
            case START_TEST_PHASE:
                processStartTestPhase((StartTestPhaseOperation) operation);
                break;
            case START_TEST:
                processStartTest((StartTestOperation) operation);
                break;
            case STOP_TEST:
                processStopTest();
                break;
            default:
                return UNSUPPORTED_OPERATION_ON_THIS_PROCESSOR;
        }
        return SUCCESS;
    }

    @Override
    protected void onProcessOperationFailure(Throwable t) {
        ExceptionReporter.report(testId, t);
    }

    private ResponseType processIntegrationTest(IntegrationTestOperation operation, SimulatorAddress sourceAddress)
            throws Exception {
        LogOperation logOperation;
        Response response;
        switch (operation.getType()) {
            case NESTED_SYNC:
                logOperation = new LogOperation("Sync nested integration test message");
                response = worker.getWorkerConnector().write(sourceAddress, logOperation);
                LOGGER.debug("Got response for sync nested message: " + response);
                return response.getFirstErrorResponseType();
            case NESTED_ASYNC:
                logOperation = new LogOperation("Async nested integration test message");
                ResponseFuture future = worker.getWorkerConnector().submitFromTest(testAddress, sourceAddress, logOperation);
                response = future.get();
                LOGGER.debug("Got response for async nested message: " + response);
                return response.getFirstErrorResponseType();
            default:
                return UNSUPPORTED_OPERATION_ON_THIS_PROCESSOR;
        }
    }

    private void processStartTestPhase(StartTestPhaseOperation operation) throws Exception {
        final TestPhase testPhase = operation.getTestPhase();

        LOGGER.info(format("%s Starting %s of %s %s", DASHES, testPhase.desc(), testId, DASHES));
        try {
            OperationThread operationThread = new OperationThread() {
                @Override
                public void run0() throws Exception {
                    try {
                        testContainer.invoke(testPhase);
                    } finally {
                        LOGGER.info(format("%s Finished %s of %s %s", DASHES, testPhase.desc(), testId, DASHES));
                        if (testPhase == getLastTestPhase()) {
                            worker.getWorkerConnector().removeTest(testAddress.getTestIndex());
                        }
                    }
                }
            };
            operationThread.start();
        } catch (Exception e) {
            LOGGER.fatal(format("Failed to execute %s of %s", testPhase.desc(), testId), e);
            throw e;
        }
    }

    private void processStartTest(StartTestOperation operation) {
        if (skipRunPhase(operation)) {
            sendPhaseCompletedOperation(TestPhase.RUN);
            return;
        }

        LOGGER.info(format("%s Starting run of %s %s", DASHES, testId, DASHES));
        OperationThread operationThread = new OperationThread() {
            @Override
            public void run0() throws Exception {
                try {
                    testContainer.invoke(TestPhase.RUN);
                } finally {
                    LOGGER.info(format("%s Completed run of %s %s", DASHES, testId, DASHES));
                }
            }
        };
        operationThread.start();
    }

    private void processStopTest() {
        LOGGER.info(format("%s Stopping %s %s", DASHES, testId, DASHES));
        testContainer.getTestContext().stop();
    }

    private boolean skipRunPhase(StartTestOperation operation) {
        if (!operation.matchesTargetType(type)) {
            TargetType targetType = operation.getTargetType();
            LOGGER.info(format("%s Skipping run of %s (%s Worker vs. %s target) %s", DASHES, testId, type, targetType, DASHES));
            return true;
        }

        if (!operation.matchesTargetWorkers(testAddress.getParent())) {
            LOGGER.info(format("%s Skipping run of %s (Worker is not on target list) %s", DASHES, testId, DASHES));
            return true;
        }
        return false;
    }

    private void sendPhaseCompletedOperation(TestPhase testPhase) {
        PhaseCompletedOperation operation = new PhaseCompletedOperation(testPhase);
        worker.getWorkerConnector().submitFromTest(testAddress, COORDINATOR, operation);
    }

    private abstract class OperationThread extends Thread {

        @Override
        @SuppressWarnings("PMD.AvoidCatchingThrowable")
        public final void run() {
            try {
                run0();
            } catch (Throwable t) {
                LOGGER.error("Error while executing test phase", t);
                ExceptionReporter.report(testId, t);
            }
        }

        abstract void run0() throws Exception;
    }
}
