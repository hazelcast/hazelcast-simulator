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

import com.hazelcast.simulator.common.TestPhase;
import com.hazelcast.simulator.common.WorkerType;
import com.hazelcast.simulator.protocol.core.Response;
import com.hazelcast.simulator.protocol.core.ResponseFuture;
import com.hazelcast.simulator.protocol.core.ResponseType;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.exception.ProcessException;
import com.hazelcast.simulator.protocol.operation.IntegrationTestOperation;
import com.hazelcast.simulator.protocol.operation.LogOperation;
import com.hazelcast.simulator.protocol.operation.OperationType;
import com.hazelcast.simulator.protocol.operation.PhaseCompletedOperation;
import com.hazelcast.simulator.protocol.operation.SimulatorOperation;
import com.hazelcast.simulator.protocol.operation.StartTestOperation;
import com.hazelcast.simulator.protocol.operation.StartTestPhaseOperation;
import com.hazelcast.simulator.coordinator.registry.TargetType;
import com.hazelcast.simulator.utils.ExceptionReporter;
import com.hazelcast.simulator.worker.Promise;
import com.hazelcast.simulator.worker.Worker;
import com.hazelcast.simulator.worker.testcontainer.TestContainer;
import org.apache.log4j.Logger;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.simulator.common.TestPhase.RUN;
import static com.hazelcast.simulator.common.TestPhase.getLastTestPhase;
import static com.hazelcast.simulator.protocol.core.ResponseType.SUCCESS;
import static com.hazelcast.simulator.protocol.core.ResponseType.UNSUPPORTED_OPERATION_ON_THIS_PROCESSOR;
import static com.hazelcast.simulator.protocol.core.SimulatorAddress.COORDINATOR;
import static java.lang.String.format;

/**
 * An {@link OperationProcessor} implementation to process {@link SimulatorOperation} instances on a Simulator Test.
 */
public class TestOperationProcessor extends AbstractOperationProcessor {

    private static final String DASHES = "---------------------------";
    private static final Logger LOGGER = Logger.getLogger(TestOperationProcessor.class);

    private static final AtomicInteger TESTS_PENDING = new AtomicInteger(0);

    private final AtomicReference<TestPhase> testPhaseReference = new AtomicReference<TestPhase>(null);

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
        return testPhaseReference.get();
    }

    public SimulatorAddress getTestAddress() {
        return testAddress;
    }

    @Override
    protected void processOperation(OperationType operationType, SimulatorOperation op,
                                    SimulatorAddress sourceAddress, Promise promise) throws Exception {
        switch (operationType) {
            case INTEGRATION_TEST:
                promise.answer(processIntegrationTest((IntegrationTestOperation) op, sourceAddress));
                return;
            case START_TEST_PHASE:
                processStartTestPhase((StartTestPhaseOperation) op);
                promise.answer(SUCCESS);
                break;
            case START_TEST:
                processStartTest((StartTestOperation) op);
                promise.answer(SUCCESS);
                break;
            case STOP_TEST:
                processStopTest();
                promise.answer(SUCCESS);
                break;
            default:
                throw new ProcessException(UNSUPPORTED_OPERATION_ON_THIS_PROCESSOR);
        }
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
                response = worker.getWorkerConnector().invoke(sourceAddress, logOperation);
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
            new OperationThread(testPhase) {
                @Override
                public void run0() throws Exception {
                    try {
                        testContainer.invoke(testPhase);
                    } finally {
                        if (testPhase == getLastTestPhase()) {
                            worker.getWorkerConnector().removeTest(testAddress.getTestIndex());
                            WorkerOperationProcessor processor =
                                    (WorkerOperationProcessor) worker.getWorkerConnector().getProcessor();
                            processor.remove(testContainer.getTestCase().getId());
                        }
                    }
                }
            }.start();
        } catch (Exception e) {
            LOGGER.fatal(format("Failed to execute %s of %s", testPhase.desc(), testId), e);
            throw e;
        }
    }

    private void processStartTest(final StartTestOperation operation) {
        if (skipRunPhase(operation)) {
            sendPhaseCompletedOperation(RUN);
            return;
        }

        LOGGER.info(format("%s Starting run of %s %s", DASHES, testId, DASHES));
        new OperationThread(RUN) {
            @Override
            public void run0() throws Exception {
                testContainer.invoke(RUN);
            }
        }.start();
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

        private final TestPhase testPhase;

        OperationThread(TestPhase testPhase) {
            this.testPhase = testPhase;
            if (!testPhaseReference.compareAndSet(null, testPhase)) {
                throw new IllegalStateException(format("Tried to start %s for test %s, but %s is still running!", testPhase,
                        testId, testPhaseReference.get()));
            }
        }

        @Override
        @SuppressWarnings("PMD.AvoidCatchingThrowable")
        public final void run() {
            try {
                run0();
                LOGGER.info(format("%s %s of %s SUCCEEDED %s ", DASHES, testPhase.desc(), testId, DASHES));
            } catch (Throwable t) {
                LOGGER.error(format("%s %s of %s FAILED %s ", DASHES, testPhase.desc(), testId, DASHES), t);
                ExceptionReporter.report(testId, t);
            } finally {
                sendPhaseCompletedOperation(testPhaseReference.getAndSet(null));
            }
        }

        abstract void run0() throws Exception;
    }
}
