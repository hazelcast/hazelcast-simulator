package com.hazelcast.simulator.protocol.processors;

import com.hazelcast.simulator.agent.workerprocess.WorkerProcessSettings;
import com.hazelcast.simulator.common.TestCase;
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
import com.hazelcast.simulator.test.TestException;
import com.hazelcast.simulator.testcontainer.TestContainer;
import com.hazelcast.simulator.testcontainer.TestContextImpl;
import com.hazelcast.simulator.testcontainer.TestPhase;
import com.hazelcast.simulator.tests.FailingTest;
import com.hazelcast.simulator.tests.SuccessTest;
import com.hazelcast.simulator.worker.Worker;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static com.hazelcast.simulator.protocol.core.ResponseType.EXCEPTION_DURING_OPERATION_EXECUTION;
import static com.hazelcast.simulator.protocol.core.ResponseType.SUCCESS;
import static com.hazelcast.simulator.protocol.core.ResponseType.UNSUPPORTED_OPERATION_ON_THIS_PROCESSOR;
import static com.hazelcast.simulator.protocol.core.SimulatorAddress.COORDINATOR;
import static com.hazelcast.simulator.protocol.operation.OperationType.getOperationType;
import static com.hazelcast.simulator.test.TestContext.LOCALHOST;
import static com.hazelcast.simulator.utils.CommonUtils.sleepMillis;
import static com.hazelcast.simulator.worker.WorkerType.MEMBER;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestOperationProcessorTest {

    private WorkerConnector workerConnector = mock(WorkerConnector.class);

    private TestOperationProcessor processor;

    @Test
    public void testProcessOperation_unsupportedOperation() throws Exception {
        createTestOperationProcessor();

        SimulatorOperation operation = new CreateWorkerOperation(Collections.<WorkerProcessSettings>emptyList(),0);
        ResponseType responseType = processor.processOperation(getOperationType(operation), operation, COORDINATOR);

        assertEquals(UNSUPPORTED_OPERATION_ON_THIS_PROCESSOR, responseType);
    }

    @Test
    public void process_IntegrationTestOperation_unsupportedOperation() throws Exception {
        createTestOperationProcessor();

        SimulatorOperation operation = new IntegrationTestOperation();
        ResponseType responseType = processor.processOperation(getOperationType(operation), operation, COORDINATOR);

        assertEquals(UNSUPPORTED_OPERATION_ON_THIS_PROCESSOR, responseType);
       // exceptionLogger.assertNoException();
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
    public void process_StartTest_failingTest() throws Exception {
        createTestOperationProcessor(FailingTest.class);

        runPhase(TestPhase.SETUP);
        runTest();

       // exceptionLogger.assertException(TestException.class);
    }

    @Test
    public void process_StartTest_noSetUp() {
        createTestOperationProcessor();

        runTest();

        // no setup was executed, so TestContext is null
       // exceptionLogger.assertException(NullPointerException.class);
    }

    @Test
    public void process_StartTest_skipRunPhase_targetTypeMismatch() {
        createTestOperationProcessor();

        StartTestOperation operation = new StartTestOperation(TargetType.CLIENT);
        ResponseType responseType = processor.process(operation, COORDINATOR);
        assertEquals(SUCCESS, responseType);

        waitForPhaseCompletion(TestPhase.RUN);

       // exceptionLogger.assertNoException();
    }

    @Test
    public void process_StartTest_skipRunPhase_notOnTargetWorkersList() {
        createTestOperationProcessor();

        List<String> targetWorkers = singletonList(new SimulatorAddress(AddressLevel.WORKER, 1, 2, 0).toString());
        StartTestOperation operation = new StartTestOperation(TargetType.ALL, targetWorkers);
        ResponseType responseType = processor.process(operation, COORDINATOR);
        assertEquals(SUCCESS, responseType);

        waitForPhaseCompletion(TestPhase.RUN);

        //exceptionLogger.assertNoException();
    }

    @Test
    public void process_StartTestPhase_failingTest() throws Exception {
        createTestOperationProcessor(FailingTest.class);

        runPhase(TestPhase.GLOBAL_VERIFY);

       // exceptionLogger.assertException(AssertionError.class);
    }

    @Test
    public void process_StartTestPhase_oldPhaseStillRunning() {
        createTestOperationProcessor();

        runPhase(TestPhase.SETUP);

        StartTestPhaseOperation operation = new StartTestPhaseOperation(TestPhase.RUN);
        processor.process(operation, COORDINATOR);

        runPhase(TestPhase.LOCAL_VERIFY, EXCEPTION_DURING_OPERATION_EXECUTION);

       // exceptionLogger.assertException(IllegalStateException.class);
    }

    @Test
    public void process_StartTestPhase_removeTest() throws Exception {
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
        ResponseType responseType = processor.process(operation, COORDINATOR);

        assertEquals(expectedResponseType, responseType);

        waitForPhaseCompletion(testPhase);
    }

    private void runTest() {
        StartTestOperation operation = new StartTestOperation();
        processor.process(operation, COORDINATOR);

        waitForPhaseCompletion(TestPhase.RUN);
    }

    private void stopTest(final int delayMs) {
        Thread stopThread = new Thread() {
            @Override
            public void run() {
                sleepMillis(delayMs);
                StopTestOperation operation = new StopTestOperation();
                processor.process(operation, COORDINATOR);
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
            TestCase testCase = new TestCase(testId);
            testCase.setProperty("class", testClass.getName());

            TestContextImpl testContext = new TestContextImpl(null, testId, LOCALHOST);
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
