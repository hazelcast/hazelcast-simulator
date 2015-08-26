package com.hazelcast.simulator.protocol.processors;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.simulator.protocol.core.ResponseType;
import com.hazelcast.simulator.protocol.operation.CreateTestOperation;
import com.hazelcast.simulator.protocol.operation.IntegrationTestOperation;
import com.hazelcast.simulator.protocol.operation.SimulatorOperation;
import com.hazelcast.simulator.test.TestCase;
import com.hazelcast.simulator.tests.SuccessTest;
import com.hazelcast.simulator.utils.ExceptionReporter;
import com.hazelcast.util.ExceptionUtil;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.exceptions.verification.WantedButNotInvoked;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.simulator.protocol.core.ResponseType.EXCEPTION_DURING_OPERATION_EXECUTION;
import static com.hazelcast.simulator.protocol.core.ResponseType.SUCCESS;
import static com.hazelcast.simulator.protocol.core.ResponseType.UNSUPPORTED_OPERATION_ON_THIS_PROCESSOR;
import static com.hazelcast.simulator.protocol.operation.OperationHandler.encodeOperation;
import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.doNothing;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.verifyNoMoreInteractions;
import static org.powermock.api.mockito.PowerMockito.verifyStatic;

@RunWith(PowerMockRunner.class)
@PrepareForTest(ExceptionReporter.class)
public class WorkerOperationProcessorTest {

    private static final Logger LOGGER = Logger.getLogger(WorkerOperationProcessorTest.class);

    private static final Class DEFAULT_TEST = SuccessTest.class;

    private final ArgumentCaptor<String> testIdCaptor = ArgumentCaptor.forClass(String.class);
    private final ArgumentCaptor<Throwable> exceptionCaptor = ArgumentCaptor.forClass(Throwable.class);

    private final Map<String, String> properties = new HashMap<String, String>();
    private final TestCase defaultTestCase = mock(TestCase.class);

    private final HazelcastInstance serverInstance = mock(HazelcastInstance.class);
    private final HazelcastInstance clientInstance = mock(HazelcastInstance.class);

    private WorkerOperationProcessor processor;

    @Before
    public void setUp() throws Exception {
        mockStatic(ExceptionReporter.class);
        doNothing().when(ExceptionReporter.class, "report", anyString(), any(Throwable.class));

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
        assertNoException();
    }

    @Test
    public void process_CreateTestOperation() throws Exception {
        ResponseType responseType = initTestCase(defaultTestCase);

        assertEquals(SUCCESS, responseType);
        assertNoException();
    }

    @Test
    public void process_CreateTestOperation_sameTestIdTwice() throws Exception {
        ResponseType responseType = initTestCase(defaultTestCase);
        assertEquals(SUCCESS, responseType);

        responseType = initTestCase(defaultTestCase);
        assertEquals(EXCEPTION_DURING_OPERATION_EXECUTION, responseType);
        assertException(IllegalStateException.class);
    }

    @Test
    public void processInitCommand_invalidTestId() {
        TestCase testCase = mock(TestCase.class);
        when(testCase.getId()).thenReturn("%&/?!");
        when(testCase.getProperties()).thenReturn(properties);

        ResponseType responseType = initTestCase(testCase);
        assertEquals(EXCEPTION_DURING_OPERATION_EXECUTION, responseType);
        assertException(IllegalArgumentException.class);
    }

    @Test
    public void processInitCommand_invalidClassPath() {
        setTestCaseClass("not.found.SuccessTest");

        ResponseType responseType = initTestCase(defaultTestCase);
        assertEquals(EXCEPTION_DURING_OPERATION_EXECUTION, responseType);
        assertException(ClassNotFoundException.class);
    }

    @Test
    public void process_CreateTestOperation_withServerInstance() throws Exception {
        processor = new WorkerOperationProcessor(serverInstance, null);

        ResponseType responseType = initTestCase(defaultTestCase);
        assertEquals(SUCCESS, responseType);
        assertNoException();
    }

    private void setTestCaseClass(String className) {
        properties.put("class", className);
    }

    private ResponseType initTestCase(TestCase testCase) {
        SimulatorOperation operation = new CreateTestOperation(testCase);
        LOGGER.debug("Serialized operation: " + encodeOperation(operation));

        return processor.process(operation);
    }

    private void assertNoException() {
        verifyStatic();
        try {
            verifyNoMoreInteractions(ExceptionReporter.class);
        } catch (Throwable t) {
            ExceptionReporter.report(testIdCaptor.capture(), exceptionCaptor.capture());
            String testId = testIdCaptor.getValue();
            Throwable throwable = exceptionCaptor.getValue();

            if (throwable != null) {
                throwable.printStackTrace();
                fail("Wanted no exception, but was: " + throwable.getClass().getSimpleName() + " in test " + testId);
                throw ExceptionUtil.rethrow(throwable);
            }
            throw ExceptionUtil.rethrow(t);
        }
    }

    private void assertException(Class<?>... exceptionTypes) {
        boolean invoked;
        long timeoutNanoTime = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
        do {
            try {
                verifyStatic(times(exceptionTypes.length));
                ExceptionReporter.report(testIdCaptor.capture(), exceptionCaptor.capture());
                invoked = true;
            } catch (WantedButNotInvoked e) {
                invoked = false;
            }
        } while (!invoked && System.nanoTime() < timeoutNanoTime);
        List<String> testIdList = testIdCaptor.getAllValues();
        List<Throwable> throwableList = exceptionCaptor.getAllValues();

        assertEquals(format("Expected %d exceptions, but found %d", exceptionTypes.length, testIdList.size()),
                exceptionTypes.length, testIdList.size());

        for (Class<?> exceptionType : exceptionTypes) {
            String testId = testIdList.remove(0);
            Throwable throwable = throwableList.remove(0);
            assertNotNull(throwable);
            String throwableClassName = throwable.getClass().getSimpleName();
            assertTrue(format("Expected %s, but was %s for test %s: %s", exceptionType.getSimpleName(), throwableClassName,
                            testId, throwable.getMessage()),
                    exceptionType.isInstance(throwable));
        }
    }
}
