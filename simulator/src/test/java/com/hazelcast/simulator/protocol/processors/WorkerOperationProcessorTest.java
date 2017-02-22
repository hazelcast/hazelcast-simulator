package com.hazelcast.simulator.protocol.processors;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.simulator.agent.workerprocess.WorkerProcessSettings;
import com.hazelcast.simulator.common.TestCase;
import com.hazelcast.simulator.common.WorkerType;
import com.hazelcast.simulator.protocol.connector.WorkerConnector;
import com.hazelcast.simulator.protocol.core.AddressLevel;
import com.hazelcast.simulator.protocol.core.ResponseType;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.operation.CreateTestOperation;
import com.hazelcast.simulator.protocol.operation.CreateWorkerOperation;
import com.hazelcast.simulator.protocol.operation.IntegrationTestOperation;
import com.hazelcast.simulator.protocol.operation.PingOperation;
import com.hazelcast.simulator.protocol.operation.SimulatorOperation;
import com.hazelcast.simulator.protocol.operation.TerminateWorkerOperation;
import com.hazelcast.simulator.tests.SuccessTest;
import com.hazelcast.simulator.utils.BashCommand;
import com.hazelcast.simulator.worker.Worker;
import com.hazelcast.simulator.worker.testcontainer.TestContainer;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.hazelcast.simulator.protocol.core.ResponseType.EXCEPTION_DURING_OPERATION_EXECUTION;
import static com.hazelcast.simulator.protocol.core.ResponseType.SUCCESS;
import static com.hazelcast.simulator.protocol.core.ResponseType.UNSUPPORTED_OPERATION_ON_THIS_PROCESSOR;
import static com.hazelcast.simulator.protocol.core.SimulatorAddress.COORDINATOR;
import static com.hazelcast.simulator.protocol.operation.OperationCodec.toJson;
import static com.hazelcast.simulator.protocol.operation.OperationType.getOperationType;
import static com.hazelcast.simulator.protocol.processors.OperationTestUtil.process;
import static com.hazelcast.simulator.protocol.processors.OperationTestUtil.processOperation;
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

    private TestCase defaultTestCase;
    private final HazelcastInstance hazelcastInstance = mock(HazelcastInstance.class);
    private final Worker worker = mock(Worker.class);
    private final SimulatorAddress workerAddress = new SimulatorAddress(AddressLevel.WORKER, 1, 1, 0);

    private Map<String, String> properties;
    private WorkerOperationProcessor processor;

    @Before
    public void before() throws Exception {
        properties = new HashMap<String, String>();
        setTestCaseClass(DEFAULT_TEST.getName());

        defaultTestCase = new TestCase(DEFAULT_TEST_ID, properties);

        when(hazelcastInstance.getUserContext()).thenReturn(new ConcurrentHashMap<String, Object>());

        TestOperationProcessor testOperationProcessor = mock(TestOperationProcessor.class);

        WorkerConnector workerConnector = mock(WorkerConnector.class);
        when(workerConnector.getTest(eq(1))).thenReturn(null).thenReturn(testOperationProcessor);
        when(workerConnector.getTest(eq(2))).thenReturn(null).thenReturn(testOperationProcessor);

        when(worker.getWorkerConnector()).thenReturn(workerConnector);

        processor = new WorkerOperationProcessor(WorkerType.MEMBER, hazelcastInstance, worker, workerAddress);
    }

    @AfterClass
    public static void afterClass() {
        new BashCommand("rm *.exception").execute();
    }

    @Test
    public void process_unsupportedOperation() throws Exception {
        SimulatorOperation operation = new CreateWorkerOperation(Collections.<WorkerProcessSettings>emptyList(), 0);
        ResponseType responseType = processOperation(processor, getOperationType(operation), operation, COORDINATOR);

        assertEquals(UNSUPPORTED_OPERATION_ON_THIS_PROCESSOR, responseType);
    }

    @Test
    public void process_IntegrationTestOperation_unsupportedOperation() throws Exception {
        SimulatorOperation operation = new IntegrationTestOperation();
        ResponseType responseType = processOperation(processor, getOperationType(operation), operation, COORDINATOR);

        assertEquals(UNSUPPORTED_OPERATION_ON_THIS_PROCESSOR, responseType);
    }

    @Test
    public void process_Ping() throws Exception {
        PingOperation operation = new PingOperation();

        ResponseType responseType = process(processor, operation, COORDINATOR);

        assertEquals(SUCCESS, responseType);
        verify(worker).getWorkerConnector();
        verifyNoMoreInteractions(worker);
    }

    @Test
    public void process_TerminateWorkers_onMemberWorker() throws Exception {
        TerminateWorkerOperation operation = new TerminateWorkerOperation(0, false);

        process(processor, operation, COORDINATOR);

        verify(worker).shutdown(false);
        verifyNoMoreInteractions(worker);
    }

    @Test
    public void process_TerminateWorkers_onClientWorker() throws Exception {
        processor = new WorkerOperationProcessor(WorkerType.JAVA_CLIENT, hazelcastInstance, worker, workerAddress);
        TerminateWorkerOperation operation = new TerminateWorkerOperation(0, false);

        process(processor, operation, COORDINATOR);

        verify(worker).shutdown(false);
        verifyNoMoreInteractions(worker);
    }

    @Test
    public void process_CreateTest() throws Exception {
        ResponseType responseType = runCreateTestOperation(defaultTestCase);

        assertEquals(SUCCESS, responseType);
        assertEquals(1, processor.getTests().size());
    }

    @Test
    public void process_CreateTest_sameTestIndexTwice() throws Exception {
        runCreateTestOperation(defaultTestCase, 1);
        List<TestContainer> original = new LinkedList<TestContainer>(processor.getTests());

        ResponseType responseType = runCreateTestOperation(defaultTestCase, 1);

        assertEquals(EXCEPTION_DURING_OPERATION_EXECUTION, responseType);
        assertEquals(original, new LinkedList<TestContainer>(processor.getTests()));
    }

    @Test
    public void process_CreateTest_sameTestIdTwice() throws Exception {
        runCreateTestOperation(defaultTestCase, 1);
        List<TestContainer> original = new LinkedList<TestContainer>(processor.getTests());

        ResponseType responseType = runCreateTestOperation(defaultTestCase, 2);

        assertEquals(EXCEPTION_DURING_OPERATION_EXECUTION, responseType);
        assertEquals(original, new LinkedList<TestContainer>(processor.getTests()));
    }

    @Test
    public void process_CreateTest_invalidClassPath() throws Exception {
        TestCase testCase = new TestCase("id")
                .setProperty("class", "not.found.SuccessTest");
        ResponseType responseType = runCreateTestOperation(testCase);

        assertEquals(EXCEPTION_DURING_OPERATION_EXECUTION, responseType);
        assertEquals(0, processor.getTests().size());
    }

    private void setTestCaseClass(String className) {
        properties.put("class", className);
    }

    private TestCase createTestCase(Class testClass, String testId) {
        setTestCaseClass(testClass.getName());
        return new TestCase(testId, properties);
    }

    private ResponseType runCreateTestOperation(TestCase testCase) throws Exception {
        return runCreateTestOperation(testCase, 1);
    }

    private ResponseType runCreateTestOperation(TestCase testCase, int testIndex) throws Exception {
        SimulatorOperation operation = new CreateTestOperation(testIndex, testCase);
        LOGGER.debug("Serialized operation: " + toJson(operation));
        return process(processor, operation, COORDINATOR);
    }
}
