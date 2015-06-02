package com.hazelcast.simulator.worker;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.simulator.test.TestCase;
import com.hazelcast.simulator.test.TestPhase;
import com.hazelcast.simulator.tests.FailingTest;
import com.hazelcast.simulator.tests.SuccessTest;
import com.hazelcast.simulator.utils.ExceptionReporter;
import com.hazelcast.simulator.worker.commands.Command;
import com.hazelcast.simulator.worker.commands.CommandRequest;
import com.hazelcast.simulator.worker.commands.CommandResponse;
import com.hazelcast.simulator.worker.commands.GenericCommand;
import com.hazelcast.simulator.worker.commands.GetBenchmarkResultsCommand;
import com.hazelcast.simulator.worker.commands.GetOperationCountCommand;
import com.hazelcast.simulator.worker.commands.GetStackTraceCommand;
import com.hazelcast.simulator.worker.commands.InitCommand;
import com.hazelcast.simulator.worker.commands.IsPhaseCompletedCommand;
import com.hazelcast.simulator.worker.commands.MessageCommand;
import com.hazelcast.simulator.worker.commands.RunCommand;
import com.hazelcast.simulator.worker.commands.StopCommand;
import com.hazelcast.util.ExceptionUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.exceptions.verification.WantedButNotInvoked;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.simulator.utils.CommonUtils.sleepMillis;
import static com.hazelcast.simulator.utils.FileUtils.deleteQuiet;
import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
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
public class WorkerCommandRequestProcessorTest {

    private static final String DEFAULT_TEST_ID = "SuccessTest";
    private static final String DEFAULT_TEST_CLASS = SuccessTest.class.getName();

    private final AtomicLong idCounter = new AtomicLong();

    private final ArgumentCaptor<String> testIdCaptor = ArgumentCaptor.forClass(String.class);
    private final ArgumentCaptor<Throwable> exceptionCaptor = ArgumentCaptor.forClass(Throwable.class);

    private final BlockingQueue<CommandRequest> requestQueue = new LinkedBlockingQueue<CommandRequest>();
    private final BlockingQueue<CommandResponse> responseQueue = new LinkedBlockingQueue<CommandResponse>();

    private final TestCase defaultTestCase = mock(TestCase.class);

    private final HazelcastInstance serverInstance = mock(HazelcastInstance.class);
    private final HazelcastInstance clientInstance = mock(HazelcastInstance.class);

    private WorkerCommandRequestProcessor requestProcessor;

    @Before
    public void setUp() throws Exception {
        mockStatic(ExceptionReporter.class);
        doNothing().when(ExceptionReporter.class, "report", anyString(), any(Throwable.class));

        when(defaultTestCase.getId()).thenReturn(DEFAULT_TEST_ID);
        when(defaultTestCase.getClassname()).thenReturn(SuccessTest.class.getName());

        when(serverInstance.getUserContext()).thenReturn(new ConcurrentHashMap<String, Object>());
        when(clientInstance.getUserContext()).thenReturn(new ConcurrentHashMap<String, Object>());

        requestProcessor = new WorkerCommandRequestProcessor(requestQueue, responseQueue, serverInstance, clientInstance);
    }

    @After
    public void tearDown() {
        requestProcessor.shutdown();
        deleteQuiet(new File("performance.txt"));
    }

    @Test
    public void process_unsupportedCommand() {
        Command command = new Command() {
        };
        handleRequestAndAssertId(command);

        assertException(RuntimeException.class);
    }

    @Test
    public void processInitCommand() {
        initTestCase(defaultTestCase);
        assertNoException();
    }

    @Test
    public void processInitCommand_sameTestTwice() {
        initTestCase(defaultTestCase);

        InitCommand initCommand = new InitCommand(defaultTestCase);
        CommandResponse response = handleRequestAndAssertId(initCommand);
        assertNull(response.result);

        assertException(IllegalStateException.class);
    }

    @Test
    public void processInitCommand_invalidTestId() {
        TestCase testCase = mock(TestCase.class);
        when(testCase.getId()).thenReturn("%&/?!");
        when(testCase.getClassname()).thenReturn(DEFAULT_TEST_CLASS);

        initTestCase(testCase);
        assertException(IllegalArgumentException.class);
    }

    @Test
    public void processInitCommand_invalidClassPath() {
        TestCase testCase = mock(TestCase.class);
        when(testCase.getId()).thenReturn(DEFAULT_TEST_ID);
        when(testCase.getClassname()).thenReturn("not.found.SuccessTest");

        initTestCase(testCase);
        assertException(ClassNotFoundException.class);
    }

    @Test
    public void processRunCommand() {
        initTestCase(defaultTestCase);
        runPhase(DEFAULT_TEST_ID, TestPhase.SETUP);
        stopTest(DEFAULT_TEST_ID, 500);
        runTest(DEFAULT_TEST_ID);

        assertNoException();
    }

    @Test
    public void processRunCommand_failingTest() {
        String testId = "FailingTest";
        TestCase testCase = mock(TestCase.class);
        when(testCase.getId()).thenReturn(testId);
        when(testCase.getClassname()).thenReturn(FailingTest.class.getName());

        initTestCase(testCase);
        runPhase(testId, TestPhase.SETUP);
        runTest(testId);

        assertException(RuntimeException.class);
    }

    @Test
    public void processRunCommand_noSetUp() {
        initTestCase(defaultTestCase);
        runTest(DEFAULT_TEST_ID);

        // no setup was executed, so TestContext is null
        assertException(NullPointerException.class);
    }

    @Test
    public void processRunCommand_testNotFound() {
        runTest("notFound");

        assertNoException();
    }

    @Test
    public void processRunCommand_passive() {
        Whitebox.setInternalState(requestProcessor, "clientInstance", (Object[]) null);

        initTestCase(defaultTestCase);
        RunCommand command = new RunCommand(DEFAULT_TEST_ID);
        command.clientOnly = true;
        handleRequestAndAssertId(command);

        waitForPhaseCompletion(DEFAULT_TEST_ID, TestPhase.RUN);

        assertNoException();
    }

    @Test
    public void processStopCommand_testNotFound() {
        stopTest("notFound", 0);
    }

    @Test
    public void processGenericCommand_testNotFound() {
        runPhase("notFound", TestPhase.SETUP);
    }

    @Test
    public void processGenericCommand_failingTest() {
        String testId = "FailingTest";
        TestCase testCase = mock(TestCase.class);
        when(testCase.getId()).thenReturn(testId);
        when(testCase.getClassname()).thenReturn(FailingTest.class.getName());

        initTestCase(testCase);
        runPhase(testId, TestPhase.GLOBAL_VERIFY);

        assertException(RuntimeException.class);
    }

    @Test
    public void processGenericCommand_oldPhaseStillRunning() {
        initTestCase(defaultTestCase);
        runPhase(DEFAULT_TEST_ID, TestPhase.SETUP);

        GenericCommand command = new GenericCommand(DEFAULT_TEST_ID, TestPhase.RUN);
        handleRequestAndAssertId(command);

        runPhase(DEFAULT_TEST_ID, TestPhase.LOCAL_VERIFY);

        assertException(IllegalStateException.class, IllegalStateException.class);
    }

    @Test
    public void processGenericCommand_removeTestAfterLocalTearDown() {
        initTestCase(defaultTestCase);
        runPhase(DEFAULT_TEST_ID, TestPhase.LOCAL_TEARDOWN);

        // we should be able to init the test again, after it has been removed
        initTestCase(defaultTestCase);

        assertNoException();
    }

    @Test
    public void processMessageCommand() {
        MessageCommand command = new MessageCommand(null);
        addRequest(command);

        assertNoException();
    }

    @Test
    public void processGetOperationCountCommand() {
        initTestCase(defaultTestCase);

        GetOperationCountCommand command = new GetOperationCountCommand();
        CommandResponse response = handleRequestAndAssertId(command);
        assertNotNull(response);
        assertEquals(0L, response.result);

        assertNoException();
    }

    @Test
    public void processGetBenchmarkResultsCommand() {
        initTestCase(defaultTestCase);

        GetBenchmarkResultsCommand command = new GetBenchmarkResultsCommand(DEFAULT_TEST_ID);
        CommandResponse response = handleRequestAndAssertId(command);
        assertNotNull(response);
        assertTrue(response.result instanceof Map);
        assertEquals(0, ((Map) response.result).size());

        assertNoException();
    }

    @Test
    public void processGetStackTraceCommand() {
        initTestCase(defaultTestCase);

        GetStackTraceCommand command = new GetStackTraceCommand(DEFAULT_TEST_ID);
        CommandResponse response = handleRequestAndAssertId(command);
        assertNotNull(response);
        assertNotNull(response.result);

        assertNoException();
    }

    @Test
    public void processGetStackTraceCommand_testNotFound() {
        GetStackTraceCommand command = new GetStackTraceCommand("notFound");
        CommandResponse response = handleRequestAndAssertId(command);
        assertNotNull(response);
        assertNull(response.result);

        assertNoException();
    }

    private void initTestCase(TestCase testCase) {
        InitCommand initCommand = new InitCommand(testCase);
        CommandResponse response = handleRequestAndAssertId(initCommand);
        assertNull(response.result);
    }

    private void runPhase(String testId, TestPhase testPhase) {
        GenericCommand command = new GenericCommand(testId, testPhase);
        handleRequestAndAssertId(command);

        waitForPhaseCompletion(testId, testPhase);
    }

    private void stopTest(final String testId, final int delayMs) {
        Thread stopThread = new Thread() {
            @Override
            public void run() {
                sleepMillis(delayMs);
                StopCommand command = new StopCommand(testId);
                addRequest(command);
            }
        };
        stopThread.start();
    }

    private void runTest(String testId) {
        RunCommand command = new RunCommand(testId);
        handleRequestAndAssertId(command);

        waitForPhaseCompletion(testId, TestPhase.RUN);
    }

    private CommandResponse handleRequestAndAssertId(Command command) {
        long id = addRequest(command);

        CommandResponse response = getResponse();
        assertNotNull(response);
        assertEquals(id, response.commandId);

        return response;
    }

    private long addRequest(Command command) {
        CommandRequest request = new CommandRequest();
        request.id = idCounter.incrementAndGet();
        request.task = command;
        requestQueue.add(request);

        return request.id;
    }

    private CommandResponse getResponse() {
        try {
            return responseQueue.take();
        } catch (InterruptedException e) {
            fail("Could not take response from queue: " + e.getMessage());
            return null;
        }
    }

    private void waitForPhaseCompletion(String testId, TestPhase testPhase) {
        IsPhaseCompletedCommand command = new IsPhaseCompletedCommand(testId, testPhase);
        CommandResponse response;
        do {
            long id = addRequest(command);
            do {
                response = getResponse();
                if (response == null) {
                    fail("Got null response on IsPhaseCompletedCommand");
                }
            } while (id != response.commandId);
        } while (Boolean.FALSE.equals(response.result));
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
