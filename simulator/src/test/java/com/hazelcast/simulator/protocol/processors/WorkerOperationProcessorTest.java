package com.hazelcast.simulator.protocol.processors;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.simulator.protocol.core.ResponseType;
import com.hazelcast.simulator.protocol.operation.CreateTestOperation;
import com.hazelcast.simulator.protocol.operation.IntegrationTestOperation;
import com.hazelcast.simulator.protocol.operation.SimulatorOperation;
import com.hazelcast.simulator.test.TestCase;
import com.hazelcast.simulator.tests.SuccessTest;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.hazelcast.simulator.protocol.core.ResponseType.EXCEPTION_DURING_OPERATION_EXECUTION;
import static com.hazelcast.simulator.protocol.core.ResponseType.SUCCESS;
import static com.hazelcast.simulator.protocol.core.ResponseType.UNSUPPORTED_OPERATION_ON_THIS_PROCESSOR;
import static com.hazelcast.simulator.protocol.operation.OperationHandler.encodeOperation;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class WorkerOperationProcessorTest {

    private static final Logger LOGGER = Logger.getLogger(WorkerOperationProcessorTest.class);

    private static final Class DEFAULT_TEST = SuccessTest.class;

    private final Map<String, String> properties = new HashMap<String, String>();
    private final TestCase defaultTestCase = mock(TestCase.class);

    private final HazelcastInstance serverInstance = mock(HazelcastInstance.class);
    private final HazelcastInstance clientInstance = mock(HazelcastInstance.class);

    private WorkerOperationProcessor processor;

    @Before
    public void setUp() {
        setTestCaseClass(DEFAULT_TEST.getName());

        when(defaultTestCase.getId()).thenReturn(DEFAULT_TEST.getSimpleName());
        when(defaultTestCase.getProperties()).thenReturn(properties);

        when(serverInstance.getUserContext()).thenReturn(new ConcurrentHashMap<String, Object>());
        when(clientInstance.getUserContext()).thenReturn(new ConcurrentHashMap<String, Object>());

        processor = new WorkerOperationProcessor(serverInstance, clientInstance);
    }

    @Test
    public void process_unsupportedCommand() throws Exception {
        SimulatorOperation operation = new IntegrationTestOperation(IntegrationTestOperation.TEST_DATA);
        ResponseType responseType = processor.processOperation(operation);

        assertEquals(UNSUPPORTED_OPERATION_ON_THIS_PROCESSOR, responseType);
    }

    @Test
    public void process_CreateTestOperation() throws Exception {
        ResponseType responseType = initTestCase(defaultTestCase);
        assertEquals(SUCCESS, responseType);
    }

    @Test
    public void process_CreateTestOperation_sameTestIdTwice() throws Exception {
        ResponseType responseType = initTestCase(defaultTestCase);
        assertEquals(SUCCESS, responseType);

        responseType = initTestCase(defaultTestCase);
        assertEquals(EXCEPTION_DURING_OPERATION_EXECUTION, responseType);
    }

    @Test
    public void processInitCommand_invalidTestId() {
        TestCase testCase = mock(TestCase.class);
        when(testCase.getId()).thenReturn("%&/?!");
        when(testCase.getProperties()).thenReturn(properties);

        ResponseType responseType = initTestCase(testCase);
        assertEquals(EXCEPTION_DURING_OPERATION_EXECUTION, responseType);
    }

    @Test
    public void processInitCommand_invalidClassPath() {
        setTestCaseClass("not.found.SuccessTest");

        ResponseType responseType = initTestCase(defaultTestCase);
        assertEquals(EXCEPTION_DURING_OPERATION_EXECUTION, responseType);
    }

    @Test
    public void process_CreateTestOperation_withServerInstance() throws Exception {
        processor = new WorkerOperationProcessor(serverInstance, null);

        ResponseType responseType = initTestCase(defaultTestCase);
        assertEquals(SUCCESS, responseType);
    }

    private void setTestCaseClass(String className) {
        properties.put("class", className);
    }

    private ResponseType initTestCase(TestCase testCase) {
        SimulatorOperation operation = new CreateTestOperation(testCase);
        LOGGER.debug("Serialized operation: " + encodeOperation(operation));

        return processor.process(operation);
    }
}
