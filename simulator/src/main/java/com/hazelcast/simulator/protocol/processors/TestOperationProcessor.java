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
import com.hazelcast.simulator.protocol.operation.OperationType;
import com.hazelcast.simulator.protocol.operation.PhaseCompletedOperation;
import com.hazelcast.simulator.protocol.operation.SimulatorOperation;
import com.hazelcast.simulator.protocol.operation.StartTestOperation;
import com.hazelcast.simulator.protocol.operation.StartTestPhaseOperation;
import com.hazelcast.simulator.test.TestPhase;
import com.hazelcast.simulator.worker.TestContainer;
import com.hazelcast.simulator.worker.Worker;
import com.hazelcast.simulator.worker.WorkerType;
import org.apache.log4j.Logger;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.simulator.protocol.core.ResponseType.SUCCESS;
import static com.hazelcast.simulator.protocol.core.ResponseType.UNSUPPORTED_OPERATION_ON_THIS_PROCESSOR;
import static com.hazelcast.simulator.protocol.core.SimulatorAddress.COORDINATOR;
import static java.lang.String.format;

/**
 * An {@link OperationProcessor} implementation to process {@link SimulatorOperation} instances on a Simulator Test.
 */
public class TestOperationProcessor extends OperationProcessor {

    private static final String DASHES = "---------------------------";
    private static final Logger LOGGER = Logger.getLogger(TestOperationProcessor.class);

    private static final AtomicInteger TESTS_PENDING = new AtomicInteger(0);
    private static final AtomicInteger TESTS_COMPLETED = new AtomicInteger(0);

    private final AtomicReference<TestPhase> testPhaseReference = new AtomicReference<TestPhase>(null);

    private final ExceptionLogger exceptionLogger;
    private final Worker worker;
    private final WorkerType type;

    private final int testIndex;
    private final String testId;
    private final TestContainer testContainer;
    private final SimulatorAddress testAddress;

    public TestOperationProcessor(ExceptionLogger exceptionLogger, Worker worker, WorkerType type, int testIndex,
                                  String testId, TestContainer testContainer, SimulatorAddress testAddress) {
        super(exceptionLogger);
        this.exceptionLogger = exceptionLogger;
        this.worker = worker;
        this.type = type;

        this.testIndex = testIndex;
        this.testId = testId;
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

    @Override
    protected ResponseType processOperation(OperationType operationType, SimulatorOperation operation,
                                            SimulatorAddress sourceAddress) throws Exception {
        switch (operationType) {
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

    private void processStartTestPhase(StartTestPhaseOperation operation) throws Exception {
        final TestPhase testPhase = operation.getTestPhase();

        try {
            OperationThread operationThread = new OperationThread(testPhase) {
                @Override
                public void doRun() throws Exception {
                    try {
                        LOGGER.info(format("%s Starting %s of %s %s", DASHES, testPhase.desc(), testId, DASHES));
                        testContainer.invoke(testPhase);
                        LOGGER.info(format("%s Finished %s of %s %s", DASHES, testPhase.desc(), testId, DASHES));
                    } finally {
                        if (testPhase == TestPhase.LOCAL_TEARDOWN) {
                            worker.getWorkerConnector().removeTest(testIndex);
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
        if (worker.startPerformanceMonitor()) {
            LOGGER.info(format("%s Starting performance monitoring %s", DASHES, DASHES));
        }

        if (operation.isPassiveMember() && type == WorkerType.MEMBER) {
            LOGGER.info(format("%s Skipping run of %s (member is passive) %s", DASHES, testId, DASHES));
            sendPhaseCompletedOperation(TestPhase.RUN);
            return;
        }

        OperationThread operationThread = new OperationThread(TestPhase.RUN) {
            @Override
            public void doRun() throws Exception {
                LOGGER.info(format("%s Starting run of %s %s", DASHES, testId, DASHES));
                testContainer.invoke(TestPhase.RUN);
                LOGGER.info(format("%s Completed run of %s %s", DASHES, testId, DASHES));

                // stop performance monitor if all tests have completed their run phase
                if (TESTS_COMPLETED.incrementAndGet() == TESTS_PENDING.get()) {
                    LOGGER.info(format("%s Stopping performance monitoring %s", DASHES, DASHES));
                    worker.shutdownPerformanceMonitor();
                }
            }
        };
        operationThread.start();
    }

    private void processStopTest() {
        LOGGER.info(format("%s Stopping %s %s", DASHES, testId, DASHES));
        testContainer.getTestContext().stop();
    }

    private void sendPhaseCompletedOperation(TestPhase testPhase) {
        PhaseCompletedOperation operation = new PhaseCompletedOperation(testPhase);
        worker.getWorkerConnector().submitFromTest(testAddress, COORDINATOR, operation);
    }

    private abstract class OperationThread extends Thread {

        public OperationThread(TestPhase testPhase) {
            if (!testPhaseReference.compareAndSet(null, testPhase)) {
                throw new IllegalStateException(format("Tried to start %s for test %s, but %s is still running!", testPhase,
                        testId, testPhaseReference.get()));
            }
        }

        @Override
        @SuppressWarnings("PMD.AvoidCatchingThrowable")
        public final void run() {
            try {
                doRun();
            } catch (Throwable t) {
                LOGGER.error("Error while executing test phase", t);
                exceptionLogger.log(t, testId);
            } finally {
                sendPhaseCompletedOperation(testPhaseReference.getAndSet(null));
            }
        }

        public abstract void doRun() throws Exception;
    }
}
