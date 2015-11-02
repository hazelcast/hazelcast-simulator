package com.hazelcast.simulator.protocol.processors;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.simulator.protocol.connector.WorkerConnector;
import com.hazelcast.simulator.protocol.core.AddressLevel;
import com.hazelcast.simulator.protocol.core.ResponseType;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.operation.CreateTestOperation;
import com.hazelcast.simulator.protocol.operation.IntegrationTestOperation;
import com.hazelcast.simulator.protocol.operation.SimulatorOperation;
import com.hazelcast.simulator.protocol.operation.TerminateWorkerOperation;
import com.hazelcast.simulator.test.TestCase;
import com.hazelcast.simulator.tests.SuccessTest;
import com.hazelcast.simulator.worker.Worker;
import com.hazelcast.simulator.worker.WorkerType;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.hazelcast.simulator.protocol.core.ResponseType.EXCEPTION_DURING_OPERATION_EXECUTION;
import static com.hazelcast.simulator.protocol.core.ResponseType.SUCCESS;
import static com.hazelcast.simulator.protocol.core.ResponseType.UNSUPPORTED_OPERATION_ON_THIS_PROCESSOR;
import static com.hazelcast.simulator.protocol.core.SimulatorAddress.COORDINATOR;
import static com.hazelcast.simulator.protocol.operation.OperationCodec.toJson;
import static com.hazelcast.simulator.protocol.operation.OperationType.getOperationType;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class WorkerOperationProcessorTest {

    private static final Logger LOGGER = Logger.getLogger(WorkerOperationProcessorTest.class);

    private static final Class DEFAULT_TEST = SuccessTest.class;
    private static final String DEFAULT_TEST_ID = DEFAULT_TEST.getSimpleName();

    private final TestCase defaultTestCase = mock(TestCase.class);
    private final TestExceptionLogger exceptionLogger = new TestExceptionLogger();
    private final HazelcastInstance hazelcastInstance = mock(HazelcastInstance.class);
    private final Worker worker = mock(Worker.class);

    private Map<String, String> properties;
    private WorkerOperationProcessor processor;

    @Before
    public void setUp() throws Exception {
        properties = new HashMap<String, String>();
        setTestCaseClass(DEFAULT_TEST.getName());

        when(defaultTestCase.getId()).thenReturn(DEFAULT_TEST_ID);
        when(defaultTestCase.getProperties()).thenReturn(properties);

        when(hazelcastInstance.getUserContext()).thenReturn(new ConcurrentHashMap<String, Object>());

        TestOperationProcessor testOperationProcessor = mock(TestOperationProcessor.class);

        WorkerConnector workerConnector = mock(WorkerConnector.class);
        when(workerConnector.getTest(eq(1))).thenReturn(null).thenReturn(testOperationProcessor);
        when(workerConnector.getTest(eq(2))).thenReturn(null).thenReturn(testOperationProcessor);

        when(worker.startPerformanceMonitor()).thenReturn(true);
        when(worker.getWorkerConnector()).thenReturn(workerConnector);

        SimulatorAddress workerAddress = new SimulatorAddress(AddressLevel.WORKER, 1, 1, 0);

        processor = new WorkerOperationProcessor(exceptionLogger, WorkerType.MEMBER, hazelcastInstance, worker, workerAddress);
    }

    @Test
    public void process_unsupportedCommand() throws Exception {
        SimulatorOperation operation = new IntegrationTestOperation(IntegrationTestOperation.TEST_DATA);
        ResponseType responseType = processor.processOperation(getOperationType(operation), operation, COORDINATOR);

        assertEquals(UNSUPPORTED_OPERATION_ON_THIS_PROCESSOR, responseType);
        exceptionLogger.assertNoException();
    }

    @Test
    public void process_TerminateWorkers() throws Exception {
        TerminateWorkerOperation operation = new TerminateWorkerOperation();
        processor.process(operation, COORDINATOR);

        verify(worker).shutdown();
        verifyNoMoreInteractions(worker);
    }

    @Test
    public void process_CreateTest() throws Exception {
        ResponseType responseType = runCreateTestOperation(defaultTestCase);

        assertEquals(SUCCESS, responseType);
        assertEquals(1, processor.getTests().size());
        exceptionLogger.assertNoException();
    }

    @Test
    public void process_CreateTest_sameTestIndexTwice() throws Exception {
        ResponseType responseType = runCreateTestOperation(defaultTestCase, 1);
        assertEquals(SUCCESS, responseType);

        responseType = runCreateTestOperation(defaultTestCase, 1);
        assertEquals(EXCEPTION_DURING_OPERATION_EXECUTION, responseType);
        exceptionLogger.assertException(IllegalStateException.class);
    }

    @Test
    public void process_CreateTest_sameTestIdTwice() throws Exception {
        ResponseType responseType = runCreateTestOperation(defaultTestCase, 1);
        assertEquals(SUCCESS, responseType);

        responseType = runCreateTestOperation(defaultTestCase, 2);
        assertEquals(EXCEPTION_DURING_OPERATION_EXECUTION, responseType);
        exceptionLogger.assertException(IllegalStateException.class);
    }

    @Test
    public void process_CreateTest_invalidTestId() {
        TestCase testCase = createTestCase(SuccessTest.class, "%&/?!");
        ResponseType responseType = runCreateTestOperation(testCase);

        assertEquals(EXCEPTION_DURING_OPERATION_EXECUTION, responseType);
        exceptionLogger.assertException(IllegalArgumentException.class);
    }

    @Test
    public void process_CreateTest_invalidClassPath() {
        setTestCaseClass("not.found.SuccessTest");
        ResponseType responseType = runCreateTestOperation(defaultTestCase);

        assertEquals(EXCEPTION_DURING_OPERATION_EXECUTION, responseType);
        exceptionLogger.assertException(ClassNotFoundException.class);
    }

    private void setTestCaseClass(String className) {
        properties.put("class", className);
    }

    private TestCase createTestCase(Class testClass, String testId) {
        setTestCaseClass(testClass.getName());
        TestCase testCase = mock(TestCase.class);
        when(testCase.getId()).thenReturn(testId);
        when(testCase.getProperties()).thenReturn(properties);

        return testCase;
    }

    private ResponseType runCreateTestOperation(TestCase testCase) {
        return runCreateTestOperation(testCase, 1);
    }


    private ResponseType runCreateTestOperation(TestCase testCase, int testIndex) {
        SimulatorOperation operation = new CreateTestOperation(testIndex, testCase);
        LOGGER.debug("Serialized operation: " + toJson(operation));

        return processor.process(operation, COORDINATOR);
    }
}
