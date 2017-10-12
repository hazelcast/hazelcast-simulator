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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.simulator.agent.workerprocess.WorkerProcessSettings;
import com.hazelcast.simulator.common.TestCase;
import com.hazelcast.simulator.common.TestPhase;
import com.hazelcast.simulator.protocol.connector.WorkerConnector;
import com.hazelcast.simulator.protocol.core.AddressLevel;
import com.hazelcast.simulator.protocol.core.ResponseType;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.operation.CreateWorkerOperation;
import com.hazelcast.simulator.protocol.operation.IntegrationTestOperation;
import com.hazelcast.simulator.protocol.operation.SimulatorOperation;
import com.hazelcast.simulator.protocol.operation.StartTestOperation;
import com.hazelcast.simulator.protocol.operation.StartTestPhaseOperation;
import com.hazelcast.simulator.protocol.operation.StopTestOperation;
import com.hazelcast.simulator.protocol.registry.TargetType;
import com.hazelcast.simulator.tests.FailingTest;
import com.hazelcast.simulator.tests.SuccessTest;
import com.hazelcast.simulator.worker.Worker;
import com.hazelcast.simulator.worker.testcontainer.TestContainer;
import com.hazelcast.simulator.worker.testcontainer.TestContextImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static com.hazelcast.simulator.TestEnvironmentUtils.setupFakeUserDir;
import static com.hazelcast.simulator.TestEnvironmentUtils.teardownFakeUserDir;
import static com.hazelcast.simulator.common.WorkerType.MEMBER;
import static com.hazelcast.simulator.protocol.core.ResponseType.EXCEPTION_DURING_OPERATION_EXECUTION;
import static com.hazelcast.simulator.protocol.core.ResponseType.SUCCESS;
import static com.hazelcast.simulator.protocol.core.ResponseType.UNSUPPORTED_OPERATION_ON_THIS_PROCESSOR;
import static com.hazelcast.simulator.protocol.core.SimulatorAddress.COORDINATOR;
import static com.hazelcast.simulator.protocol.operation.OperationType.getOperationType;
import static com.hazelcast.simulator.protocol.processors.OperationTestUtil.process;
import static com.hazelcast.simulator.protocol.processors.OperationTestUtil.processOperation;
import static com.hazelcast.simulator.utils.CommonUtils.sleepMillis;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestOperationProcessorTest {

    private WorkerConnector workerConnector = mock(WorkerConnector.class);

    private TestOperationProcessor processor;

    @Before
    public void before(){
        setupFakeUserDir();
    }

    @After
    public void after() {
        teardownFakeUserDir();
    }

    @Test
    public void testProcessOperation_unsupportedOperation() {
        createTestOperationProcessor();

        SimulatorOperation operation = new CreateWorkerOperation(Collections.<WorkerProcessSettings>emptyList(), 0);
        ResponseType responseType = processOperation(processor, getOperationType(operation), operation, COORDINATOR);

        assertEquals(UNSUPPORTED_OPERATION_ON_THIS_PROCESSOR, responseType);
    }

    @Test
    public void process_IntegrationTestOperation_unsupportedOperation() {
        createTestOperationProcessor();

        SimulatorOperation operation = new IntegrationTestOperation();
        ResponseType responseType = processOperation(processor, getOperationType(operation), operation, COORDINATOR);

        assertEquals(UNSUPPORTED_OPERATION_ON_THIS_PROCESSOR, responseType);
        //exceptionLogger.assertNoException();
    }

    @Test
    public void process_StartTest() {
        createTestOperationProcessor();

        runPhase(TestPhase.SETUP);
        stopTest(500);
        runTest();

        //exceptionLogger.assertNoException();
    }

    @Test
    public void process_StartTest_failingTest() {
        createTestOperationProcessor(FailingTest.class);

        runPhase(TestPhase.SETUP);
        runTest();

        //exceptionLogger.assertException(TestException.class);
    }

    @Test
    public void process_StartTest_noSetUp() {
        createTestOperationProcessor();

        runTest();

        // no setup was executed, so TestContext is null
        //exceptionLogger.assertException(NullPointerException.class);
    }

    @Test
    public void process_StartTest_skipRunPhase_targetTypeMismatch() {
        createTestOperationProcessor();

        StartTestOperation operation = new StartTestOperation(TargetType.CLIENT);
        ResponseType responseType = process(processor, operation, COORDINATOR);
        assertEquals(SUCCESS, responseType);

        waitForPhaseCompletion(TestPhase.RUN);

        //exceptionLogger.assertNoException();
    }

    @Test
    public void process_StartTest_skipRunPhase_notOnTargetWorkersList() {
        createTestOperationProcessor();

        List<String> targetWorkers = singletonList(new SimulatorAddress(AddressLevel.WORKER, 1, 2, 0).toString());
        StartTestOperation operation = new StartTestOperation(TargetType.ALL, targetWorkers, false);
        ResponseType responseType = process(processor, operation, COORDINATOR);
        assertEquals(SUCCESS, responseType);

        waitForPhaseCompletion(TestPhase.RUN);

        //exceptionLogger.assertNoException();
    }

    @Test
    public void process_StartTestPhase_failingTest() {
        createTestOperationProcessor(FailingTest.class);

        runPhase(TestPhase.GLOBAL_VERIFY);

        //exceptionLogger.assertException(AssertionError.class);
    }

    @Test
    public void process_StartTestPhase_oldPhaseStillRunning() {
        createTestOperationProcessor();

        runPhase(TestPhase.SETUP);

        StartTestPhaseOperation operation = new StartTestPhaseOperation(TestPhase.RUN);
        process(processor, operation, COORDINATOR);

        runPhase(TestPhase.LOCAL_VERIFY, EXCEPTION_DURING_OPERATION_EXECUTION);

        //exceptionLogger.assertException(IllegalStateException.class);
    }

    @Test
    public void process_StartTestPhase_removeTest() {
        createTestOperationProcessor();

        runPhase(TestPhase.LOCAL_TEARDOWN);

        //exceptionLogger.assertNoException();
        verify(workerConnector).removeTest(1);
    }

    private void runPhase(TestPhase testPhase) {
        runPhase(testPhase, SUCCESS);
    }

    private void runPhase(TestPhase testPhase, ResponseType expectedResponseType) {
        StartTestPhaseOperation operation = new StartTestPhaseOperation(testPhase);
        ResponseType responseType = process(processor, operation, COORDINATOR);

        assertEquals(expectedResponseType, responseType);

        waitForPhaseCompletion(testPhase);
    }

    private void runTest() {
        StartTestOperation operation = new StartTestOperation();
        process(processor, operation, COORDINATOR);

        waitForPhaseCompletion(TestPhase.RUN);
    }

    @SuppressWarnings("SameParameterValue")
    private void stopTest(final int delayMs) {
        Thread stopThread = new Thread() {
            @Override
            public void run() {
                sleepMillis(delayMs);
                StopTestOperation operation = new StopTestOperation();
                try {
                    process(processor, operation, COORDINATOR);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        };
        stopThread.start();
    }

    private void waitForPhaseCompletion(TestPhase testPhase) {
        while (processor.getTestPhase() == testPhase) {
            sleepMillis(100);
        }
    }

    private void createTestOperationProcessor() {
        createTestOperationProcessor(SuccessTest.class);
    }

    private void createTestOperationProcessor(Class<?> testClass) {
        try {
            Worker worker = mock(Worker.class);
            when(worker.getWorkerConnector()).thenReturn(workerConnector);

            String testId = testClass.getSimpleName();
            TestCase testCase = new TestCase(testId)
                    .setProperty("class", testClass);

            TestContextImpl testContext = new TestContextImpl(
                    mock(HazelcastInstance.class), testCase.getId(), "localhost", mock(WorkerConnector.class));
            TestContainer testContainer = new TestContainer(testContext, testCase);
            SimulatorAddress testAddress = new SimulatorAddress(AddressLevel.TEST, 1, 1, 1);

            TestOperationProcessor.resetPendingTests();
            processor = new TestOperationProcessor(worker, MEMBER, testContainer, testAddress);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }
}
