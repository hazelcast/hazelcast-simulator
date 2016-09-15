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
package com.hazelcast.simulator.worker.testcontainer;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.simulator.common.TestCase;
import com.hazelcast.simulator.common.TestPhase;
import com.hazelcast.simulator.common.WorkerType;
import com.hazelcast.simulator.protocol.connector.ServerConnector;
import com.hazelcast.simulator.protocol.core.AddressLevel;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.exception.ProcessException;
import com.hazelcast.simulator.protocol.operation.CreateTestOperation;
import com.hazelcast.simulator.protocol.operation.PhaseCompletedOperation;
import com.hazelcast.simulator.protocol.operation.StartTestOperation;
import com.hazelcast.simulator.protocol.registry.TargetType;
import com.hazelcast.simulator.utils.ExceptionReporter;
import com.hazelcast.simulator.utils.WaitableFuture;
import org.apache.log4j.Logger;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;

import static com.hazelcast.simulator.common.TestPhase.getLastTestPhase;
import static com.hazelcast.simulator.protocol.core.ResponseType.FAILURE_TEST_NOT_FOUND;
import static com.hazelcast.simulator.protocol.core.SimulatorAddress.COORDINATOR;
import static com.hazelcast.simulator.utils.TestUtils.getUserContextKeyFromTestId;
import static java.lang.String.format;

public class TestContainerManager {

    private static final String DASHES = "---------------------------";
    private static final Logger LOGGER = Logger.getLogger(TestContainerManager.class);

    private final ConcurrentMap<Integer, TestContainer> tests = new ConcurrentHashMap<Integer, TestContainer>();
    private final HazelcastInstance hazelcastInstance;
    private final SimulatorAddress workerAddress;
    private final String publicIpAddress;
    private final ServerConnector workerConnector;
    private final WorkerType workerType;

    public TestContainerManager(
            HazelcastInstance hz,
            SimulatorAddress workerAddress,
            String publicIpAddress,
            ServerConnector workerConnector,
            WorkerType type) {
        this.hazelcastInstance = hz;
        this.workerAddress = workerAddress;
        this.publicIpAddress = publicIpAddress;
        this.workerConnector = workerConnector;
        this.workerType = type;
    }

    public Collection<TestContainer> getTests() {
        return tests.values();
    }

    public TestContainer get(int testIndex) {
        return tests.get(testIndex);
    }

    public void createTest(CreateTestOperation op) {
        TestCase testCase = op.getTestCase();
        LOGGER.info(testCase.toString());

        int testIndex = op.getTestIndex();

        String testId = testCase.getId();
        LOGGER.info(format("%s Initializing test %s %s%n%s", DASHES, testId, DASHES, testCase));

        TestContextImpl testContext = new TestContextImpl(hazelcastInstance, testId, publicIpAddress, workerConnector);

        TestContainer container = new TestContainer(testContext, testCase);

        tests.put(testIndex, container);

        if (workerType == WorkerType.MEMBER) {
            hazelcastInstance.getUserContext().put(getUserContextKeyFromTestId(testId), container.getTestInstance());
        }
    }

    public Future startTestPhase(final SimulatorAddress testAddress, final TestPhase testPhase) throws Exception {
        final TestContainer container = getExistingContainer(testAddress);

        final String testId = container.getTestCase().getId();

        LOGGER.info(format("%s Starting %s of %s %s", DASHES, testPhase.desc(), testId, DASHES));
        try {
            OperationThread thread = new OperationThread(container, testPhase, testAddress) {
                @Override
                public void run0() throws Exception {
                    try {
                        container.invoke(testPhase);
                    } finally {
                        LOGGER.info(format("%s Finished %s of %s %s", DASHES, testPhase.desc(), testId, DASHES));
                        if (testPhase == getLastTestPhase()) {
                            tests.remove(testAddress.getTestIndex());
                        }
                    }
                }
            };
            thread.start();
            return thread.future;
        } catch (Exception e) {
            LOGGER.fatal(format("Failed to execute %s of %s", testPhase.desc(), testId), e);
            throw e;
        }
    }

    public Future start(StartTestOperation operation, SimulatorAddress testAddress) {
        final TestPhase phase = operation.isWarmup() ? TestPhase.WARMUP : TestPhase.RUN;

        final TestContainer container = getExistingContainer(testAddress);

        if (skipRunPhase(operation, container.getTestCase().getId())) {
            sendPhaseCompleted(phase, testAddress);
            new WaitableFuture(null);
        }

        final String testId = container.getTestCase().getId();
        LOGGER.info(format("%s Starting run of %s %s", DASHES, testId, DASHES));

        OperationThread t = new OperationThread(container, phase, testAddress) {
            @Override
            public void run0() throws Exception {
                try {
                    container.invoke(phase);
                } finally {
                    LOGGER.info(format("%s Completed run of %s %s", DASHES, testId, DASHES));
                }
            }
        };
        t.start();
        return t.future;
    }

    private TestContainer getExistingContainer(SimulatorAddress testAddress) {
        final TestContainer container = tests.get(testAddress.getTestIndex());
        if (container == null) {
            throw new ProcessException("no test found for [" + testAddress + "]", FAILURE_TEST_NOT_FOUND);
        }
        return container;
    }

    private boolean skipRunPhase(StartTestOperation operation, String testId) {
        if (!operation.matchesTargetType(workerType)) {
            TargetType targetType = operation.getTargetType();
            LOGGER.info(format("%s Skipping run of %s (%s Worker vs. %s target) %s",
                    DASHES, testId, workerType, targetType, DASHES));
            return true;
        }

        if (!operation.matchesTargetWorkers(workerAddress)) {
            LOGGER.info(format("%s Skipping run of %s (Worker is not on target list) %s", DASHES, testId, DASHES));
            return true;
        }
        return false;
    }

    public void stop(SimulatorAddress testAddress) {
        TestContainer testContainer = tests.get(testAddress.getTestIndex());
        if (testContainer == null) {
            return;
        }

        LOGGER.info(format("%s Stopping %s %s", DASHES, testContainer.getTestCase().getId(), DASHES));
        testContainer.getTestContext().stop();
    }

    private void sendPhaseCompleted(TestPhase testPhase, SimulatorAddress testAddress) {
        PhaseCompletedOperation operation = new PhaseCompletedOperation(testPhase);
        SimulatorAddress concreteTestAddress = new SimulatorAddress(
                AddressLevel.TEST, workerAddress.getAgentIndex(), workerAddress.getWorkerIndex(), testAddress.getTestIndex());
        workerConnector.submit(concreteTestAddress, COORDINATOR, operation);
    }

    private abstract class OperationThread extends Thread {

        private final TestContainer testContainer;
        private final SimulatorAddress testAddress;
        private final WaitableFuture future = new WaitableFuture();

        OperationThread(TestContainer container, TestPhase phase, SimulatorAddress testAddress) {
            this.testContainer = container;
            this.testAddress = testAddress;

            if (!container.trySetTestPhase(phase)) {
                throw new IllegalStateException(format("Tried to start %s for test %s, but %s is still running!", phase,
                        container.getTestCase().getId(), container.getTestPhase()));
            }
        }

        @Override
        @SuppressWarnings("PMD.AvoidCatchingThrowable")
        public final void run() {
            try {
                run0();
            } catch (Throwable t) {
                LOGGER.error("Error while executing test phase", t);
                ExceptionReporter.report(testContainer.getTestCase().getId(), t);
            } finally {
                sendPhaseCompleted(testContainer.unsetPhase(), testAddress);
                future.complete(null);
            }
        }

        abstract void run0() throws Exception;
    }
}
