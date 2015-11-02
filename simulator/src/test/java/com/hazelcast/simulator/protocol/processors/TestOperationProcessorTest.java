package com.hazelcast.simulator.protocol.processors;

import com.hazelcast.simulator.protocol.connector.WorkerConnector;
import com.hazelcast.simulator.protocol.core.AddressLevel;
import com.hazelcast.simulator.protocol.core.ResponseType;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.operation.IntegrationTestOperation;
import com.hazelcast.simulator.protocol.operation.SimulatorOperation;
import com.hazelcast.simulator.protocol.operation.StartTestOperation;
import com.hazelcast.simulator.protocol.operation.StartTestPhaseOperation;
import com.hazelcast.simulator.protocol.operation.StopTestOperation;
import com.hazelcast.simulator.test.TestCase;
import com.hazelcast.simulator.test.TestException;
import com.hazelcast.simulator.test.TestPhase;
import com.hazelcast.simulator.tests.FailingTest;
import com.hazelcast.simulator.tests.SuccessTest;
import com.hazelcast.simulator.worker.TestContainer;
import com.hazelcast.simulator.worker.TestContextImpl;
import com.hazelcast.simulator.worker.Worker;
import com.hazelcast.simulator.worker.WorkerType;
import org.junit.Test;

import static com.hazelcast.simulator.protocol.core.ResponseType.EXCEPTION_DURING_OPERATION_EXECUTION;
import static com.hazelcast.simulator.protocol.core.ResponseType.SUCCESS;
import static com.hazelcast.simulator.protocol.core.ResponseType.UNSUPPORTED_OPERATION_ON_THIS_PROCESSOR;
import static com.hazelcast.simulator.protocol.core.SimulatorAddress.COORDINATOR;
import static com.hazelcast.simulator.protocol.operation.OperationType.getOperationType;
import static com.hazelcast.simulator.utils.CommonUtils.sleepMillis;
import static com.hazelcast.simulator.utils.PropertyBindingSupport.bindProperties;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestOperationProcessorTest {

    private final TestExceptionLogger exceptionLogger = new TestExceptionLogger();

    private WorkerConnector workerConnector = mock(WorkerConnector.class);

    private TestOperationProcessor processor;

    @Test
    public void testProcessOperation_UnsupportedOperation() throws Exception {
        createTestOperationProcessor();

        SimulatorOperation operation = new IntegrationTestOperation(IntegrationTestOperation.TEST_DATA);
        ResponseType responseType = processor.processOperation(getOperationType(operation), operation, COORDINATOR);

        assertEquals(UNSUPPORTED_OPERATION_ON_THIS_PROCESSOR, responseType);
    }

    @Test
    public void process_StartTest() {
        createTestOperationProcessor();

        runPhase(TestPhase.SETUP);
        stopTest(500);
        runTest();

        exceptionLogger.assertNoException();
    }

    @Test
    public void process_StartTest_failingTest() throws Exception {
        createTestOperationProcessor(FailingTest.class);

        runPhase(TestPhase.SETUP);
        runTest();

        exceptionLogger.assertException(TestException.class);
    }

    @Test
    public void process_StartTest_noSetUp() {
        createTestOperationProcessor();

        runTest();

        // no setup was executed, so TestContext is null
        exceptionLogger.assertException(NullPointerException.class);
    }

    @Test
    public void process_StartTest_passiveMember() {
        createTestOperationProcessor();

        StartTestOperation operation = new StartTestOperation(true);
        ResponseType responseType = processor.process(operation, COORDINATOR);
        assertEquals(SUCCESS, responseType);

        waitForPhaseCompletion(TestPhase.RUN);

        exceptionLogger.assertNoException();
    }

    @Test
    public void process_StartTestPhase_failingTest() throws Exception {
        createTestOperationProcessor(FailingTest.class);

        runPhase(TestPhase.GLOBAL_VERIFY);

        exceptionLogger.assertException(AssertionError.class);
    }

    @Test
    public void process_StartTestPhase_oldPhaseStillRunning() {
        createTestOperationProcessor();

        runPhase(TestPhase.SETUP);

        StartTestPhaseOperation operation = new StartTestPhaseOperation(TestPhase.RUN);
        processor.process(operation, COORDINATOR);

        runPhase(TestPhase.LOCAL_VERIFY, EXCEPTION_DURING_OPERATION_EXECUTION);

        exceptionLogger.assertException(IllegalStateException.class);
    }

    @Test
    public void process_StartTestPhase_removeTest() throws Exception {
        createTestOperationProcessor();

        runPhase(TestPhase.LOCAL_TEARDOWN);

        exceptionLogger.assertNoException();

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
        StartTestOperation operation = new StartTestOperation(false);
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
            when(worker.startPerformanceMonitor()).thenReturn(true).thenReturn(false);

            String testId = testClass.getSimpleName();
            TestCase testCase = new TestCase(testId);
            testCase.setProperty("class", testClass.getName());

            Object testInstance = getClass().getClassLoader().loadClass(testCase.getClassname()).newInstance();
            bindProperties(testInstance, testCase, TestContainer.OPTIONAL_TEST_PROPERTIES);
            TestContextImpl testContext = new TestContextImpl(testId, null);
            TestContainer testContainer = new TestContainer(testInstance, testContext, testCase);
            SimulatorAddress testAddress = new SimulatorAddress(AddressLevel.TEST, 1, 1, 1);

            TestOperationProcessor.resetPendingTests();
            processor = new TestOperationProcessor(exceptionLogger, worker, WorkerType.MEMBER, 1, testId, testContainer,
                    testAddress);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }
}
