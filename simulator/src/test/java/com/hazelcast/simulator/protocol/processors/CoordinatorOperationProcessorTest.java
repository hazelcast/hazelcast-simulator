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

import com.hazelcast.simulator.agent.workerprocess.WorkerProcessSettings;
import com.hazelcast.simulator.common.FailureType;
import com.hazelcast.simulator.common.TestPhase;
import com.hazelcast.simulator.common.WorkerType;
import com.hazelcast.simulator.coordinator.FailureCollector;
import com.hazelcast.simulator.coordinator.FailureListener;
import com.hazelcast.simulator.coordinator.PerformanceStatsCollector;
import com.hazelcast.simulator.coordinator.TestPhaseListener;
import com.hazelcast.simulator.coordinator.TestPhaseListeners;
import com.hazelcast.simulator.protocol.StubPromise;
import com.hazelcast.simulator.protocol.core.ResponseType;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.exception.ProcessException;
import com.hazelcast.simulator.protocol.operation.FailureOperation;
import com.hazelcast.simulator.protocol.operation.IntegrationTestOperation;
import com.hazelcast.simulator.protocol.operation.PerformanceStatsOperation;
import com.hazelcast.simulator.protocol.operation.PhaseCompletedOperation;
import com.hazelcast.simulator.protocol.operation.SimulatorOperation;
import com.hazelcast.simulator.protocol.registry.ComponentRegistry;
import com.hazelcast.simulator.test.TestException;
import com.hazelcast.simulator.utils.TestUtils;
import com.hazelcast.simulator.worker.performance.PerformanceStats;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.HashMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.simulator.coordinator.PerformanceStatsCollector.LATENCY_FORMAT_LENGTH;
import static com.hazelcast.simulator.coordinator.PerformanceStatsCollector.OPERATION_COUNT_FORMAT_LENGTH;
import static com.hazelcast.simulator.coordinator.PerformanceStatsCollector.THROUGHPUT_FORMAT_LENGTH;
import static com.hazelcast.simulator.protocol.core.AddressLevel.TEST;
import static com.hazelcast.simulator.protocol.core.AddressLevel.WORKER;
import static com.hazelcast.simulator.protocol.core.ResponseType.EXCEPTION_DURING_OPERATION_EXECUTION;
import static com.hazelcast.simulator.protocol.core.ResponseType.SUCCESS;
import static com.hazelcast.simulator.protocol.core.ResponseType.UNSUPPORTED_OPERATION_ON_THIS_PROCESSOR;
import static com.hazelcast.simulator.protocol.core.SimulatorAddress.COORDINATOR;
import static com.hazelcast.simulator.protocol.operation.OperationType.getOperationType;
import static com.hazelcast.simulator.protocol.processors.OperationTestUtil.process;
import static com.hazelcast.simulator.utils.FileUtils.deleteQuiet;
import static com.hazelcast.simulator.utils.FormatUtils.formatDouble;
import static com.hazelcast.simulator.utils.FormatUtils.formatLong;
import static java.lang.String.format;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CoordinatorOperationProcessorTest implements FailureListener {

    private BlockingQueue<FailureOperation> failureOperations = new LinkedBlockingQueue<FailureOperation>();

    private SimulatorAddress workerAddress;

    private TestPhaseListeners testPhaseListeners;
    private PerformanceStatsCollector performanceStatsCollector;
    private FailureCollector failureCollector;

    private CoordinatorOperationProcessor processor;
    private File outputDirectory;

    @Before
    public void before() {
        testPhaseListeners = new TestPhaseListeners();
        performanceStatsCollector = new PerformanceStatsCollector();

        ComponentRegistry componentRegistry = new ComponentRegistry();
        SimulatorAddress agentAddress = componentRegistry.addAgent("192.168.0.1", "192.168.0.1").getAddress();

        workerAddress = new SimulatorAddress(WORKER, 1, 1, 0);

        componentRegistry.addWorkers(agentAddress, singletonList(
                new WorkerProcessSettings(
                        workerAddress.getWorkerIndex(),
                        WorkerType.MEMBER,
                        "outofthebox",
                        "",
                        0,
                        new HashMap<String, String>())));

        outputDirectory = TestUtils.createTmpDirectory();
        failureCollector = new FailureCollector(outputDirectory, componentRegistry);

        processor = new CoordinatorOperationProcessor(null, failureCollector, testPhaseListeners, performanceStatsCollector);
    }

    @After
    public void tearDown() {
        deleteQuiet(outputDirectory);
    }

    @Override
    public void onFailure(FailureOperation failure, boolean isFinishedFailure, boolean isCritical) {
        failureOperations.add(failure);
    }

    @Test
    public void testProcessOperation_unsupportedOperation() throws Exception {
        SimulatorOperation operation = new IntegrationTestOperation();

        try {
            processor.processOperation(getOperationType(operation), operation, COORDINATOR, new StubPromise());
            fail();
        } catch (ProcessException e) {
            assertEquals(UNSUPPORTED_OPERATION_ON_THIS_PROCESSOR, e.getResponseType());
        }
    }

    @Test
    public void processFailureOperation() throws Exception {
        failureCollector.addListener(this);

        TestException exception = new TestException("expected exception");
        FailureOperation operation = new FailureOperation("CoordinatorOperationProcessorTest", FailureType.WORKER_OOME,
                workerAddress, workerAddress.getParent().toString(), exception);
        ResponseType responseType = process(processor, operation, workerAddress);

        assertEquals(SUCCESS, responseType);
        assertEquals(1, failureOperations.size());

        FailureOperation failure = failureOperations.poll();
        assertNull(failure.getTestId());
        assertExceptionClassInFailure(failure, TestException.class);
    }

    @Test
    public void processPhaseCompletion() throws Exception {
        final AtomicInteger phaseCompleted = new AtomicInteger();

        PhaseCompletedOperation operation = new PhaseCompletedOperation(TestPhase.LOCAL_TEARDOWN);

        testPhaseListeners.addListener(1, new TestPhaseListener() {
            @Override
            public void onCompletion(TestPhase testPhase, SimulatorAddress workerAddress) {
                if (testPhase.equals(TestPhase.LOCAL_TEARDOWN)) {
                    phaseCompleted.incrementAndGet();
                }
            }
        });

        ResponseType responseType = process(processor, operation, new SimulatorAddress(TEST, 1, 1, 1));
        assertEquals(SUCCESS, responseType);
        assertEquals(1, phaseCompleted.get());

        responseType = process(processor, operation, new SimulatorAddress(TEST, 1, 2, 1));
        assertEquals(SUCCESS, responseType);
        assertEquals(2, phaseCompleted.get());
    }

    @Test
    public void processPhaseCompletion_withOperationFromWorker() throws Exception {
        PhaseCompletedOperation operation = new PhaseCompletedOperation(TestPhase.LOCAL_TEARDOWN);
        ResponseType responseType = process(processor, operation, workerAddress);

        assertEquals(EXCEPTION_DURING_OPERATION_EXECUTION, responseType);
    }

    @Test
    public void processPerformanceStats() throws Exception {
        PerformanceStatsOperation operation = new PerformanceStatsOperation();
        operation.addPerformanceStats("testId", new PerformanceStats(1000, 50.0, 1234.56, 33000.0d, 23000, 42000));

        ResponseType responseType = process(processor, operation, workerAddress);
        assertEquals(SUCCESS, responseType);

        String performanceNumbers = performanceStatsCollector.formatIntervalPerformanceNumbers("testId");
        assertTrue(performanceNumbers.contains(formatLong(1000, OPERATION_COUNT_FORMAT_LENGTH)));
        assertTrue(performanceNumbers.contains(formatDouble(50, THROUGHPUT_FORMAT_LENGTH)));
        assertTrue(performanceNumbers.contains(formatLong(23, LATENCY_FORMAT_LENGTH)));
        assertTrue(performanceNumbers.contains(formatLong(33, LATENCY_FORMAT_LENGTH)));
        assertTrue(performanceNumbers.contains(formatLong(42, LATENCY_FORMAT_LENGTH)));
    }

    private static void assertExceptionClassInFailure(FailureOperation failure, Class<? extends Throwable> failureClass) {
        assertTrue(format("Expected cause to start with %s, but was %s", failureClass.getCanonicalName(), failure.getCause()),
                failure.getCause().startsWith(failureClass.getCanonicalName()));
    }
}
