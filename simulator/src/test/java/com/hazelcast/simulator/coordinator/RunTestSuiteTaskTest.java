package com.hazelcast.simulator.coordinator;

import com.hazelcast.simulator.agent.workerprocess.WorkerProcessSettings;
import com.hazelcast.simulator.common.FailureType;
import com.hazelcast.simulator.common.SimulatorProperties;
import com.hazelcast.simulator.common.TestCase;
import com.hazelcast.simulator.common.TestSuite;
import com.hazelcast.simulator.protocol.connector.CoordinatorConnector;
import com.hazelcast.simulator.protocol.core.Response;
import com.hazelcast.simulator.protocol.core.ResponseType;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.operation.CreateTestOperation;
import com.hazelcast.simulator.protocol.operation.FailureOperation;
import com.hazelcast.simulator.protocol.operation.SimulatorOperation;
import com.hazelcast.simulator.protocol.operation.StartTestOperation;
import com.hazelcast.simulator.protocol.operation.StartTestPhaseOperation;
import com.hazelcast.simulator.protocol.operation.StopTestOperation;
import com.hazelcast.simulator.protocol.registry.ComponentRegistry;
import com.hazelcast.simulator.protocol.registry.TargetType;
import com.hazelcast.simulator.protocol.registry.TestData;
import com.hazelcast.simulator.testcontainer.TestPhase;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static com.hazelcast.simulator.TestEnvironmentUtils.resetUserDir;
import static com.hazelcast.simulator.TestEnvironmentUtils.setDistributionUserDir;
import static com.hazelcast.simulator.common.FailureType.WORKER_EXCEPTION;
import static com.hazelcast.simulator.common.FailureType.WORKER_FINISHED;
import static com.hazelcast.simulator.protocol.core.AddressLevel.WORKER;
import static com.hazelcast.simulator.testcontainer.TestPhase.RUN;
import static com.hazelcast.simulator.testcontainer.TestPhase.WARMUP;
import static com.hazelcast.simulator.utils.CommonUtils.await;
import static com.hazelcast.simulator.utils.CommonUtils.sleepMillis;
import static com.hazelcast.simulator.utils.FileUtils.deleteQuiet;
import static com.hazelcast.simulator.utils.FileUtils.ensureExistingDirectory;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RunTestSuiteTaskTest {

    private CountDownLatch finishWorkerLatch = new CountDownLatch(1);

    private File outputDirectory;
    private TestSuite testSuite;
    private FailureOperation criticalFailureOperation;

    private SimulatorProperties simulatorProperties;
    private ComponentRegistry componentRegistry;
    private FailureCollector failureCollector;
    private RemoteClient remoteClient;

    private boolean parallel = false;
    private boolean verifyEnabled = true;
    private boolean monitorPerformance = false;

    @BeforeClass
    public static void prepareEnvironment() {
        setDistributionUserDir();
    }

    @AfterClass
    public static void resetEnvironment() {
        resetUserDir();
    }

    @Before
    public void setUp() {
        TestCase testCase1 = new TestCase("CoordinatorTest1");
        TestCase testCase2 = new TestCase("CoordinatorTest2");

        testSuite = new TestSuite();
        testSuite.addTest(testCase1);
        testSuite.addTest(testCase2);

        outputDirectory = ensureExistingDirectory("RunTestSuiteTaskTest");

        SimulatorAddress address = new SimulatorAddress(WORKER, 1, 1, 0);
        criticalFailureOperation = new FailureOperation("expected critical failure", WORKER_EXCEPTION, address, "127.0.0.1",
                "127.0.0.1:5701", "workerId", "CoordinatorTest1", testSuite, "stacktrace");

        simulatorProperties = new SimulatorProperties();

        Response response = new Response(1, SimulatorAddress.COORDINATOR, address, ResponseType.SUCCESS);

        CoordinatorConnector connector = mock(CoordinatorConnector.class);
        when(connector.write(any(SimulatorAddress.class), any(SimulatorOperation.class))).thenReturn(response);

        remoteClient = mock(RemoteClient.class);
        when(remoteClient.getCoordinatorConnector()).thenReturn(connector);
    }

    @After
    public void cleanUp() {
        deleteQuiet(outputDirectory);
    }

    @Test
    public void runTestSuiteParallel_waitForTestCase_and_duration() {
        testSuite.setWaitForTestCase(true);
        testSuite.setDurationSeconds(3);
        parallel = true;

        RunTestSuiteTask task = createRunTestSuiteTask();
        task.run();

        verifyRemoteClient();
    }

    @Test
    public void runTestSuiteParallel_waitForTestCase_noVerify() {
        testSuite.setWaitForTestCase(true);
        testSuite.setDurationSeconds(0);
        parallel = true;
        verifyEnabled = false;

        RunTestSuiteTask task = createRunTestSuiteTask();
        task.run();

        verifyRemoteClient();
    }

    @Test
    public void runTestSuiteParallel_performanceMonitorEnabled() {
        testSuite.setDurationSeconds(4);
        parallel = true;
        monitorPerformance = true;

        RunTestSuiteTask task = createRunTestSuiteTask();
        task.run();

        verifyRemoteClient();
    }

    @Test
    public void runTestSuiteParallel_withTargetCount() {
        testSuite.setWaitForTestCase(true);
        testSuite.setDurationSeconds(0);
        parallel = true;
        verifyEnabled = false;

        RunTestSuiteTask task = createRunTestSuiteTask(1);
        task.run();

        verifyRemoteClient();
    }

    @Test
    public void runTestSuiteParallel_withWarmup() {
        testSuite.setDurationSeconds(1);
        testSuite.setWarmupDurationSeconds(1);
        parallel = true;
        verifyEnabled = false;

        RunTestSuiteTask task = createRunTestSuiteTask();
        task.run();

        verifyRemoteClient();
    }

    @Test
    public void runTestSuiteParallel_withWarmup_waitForTestCase() {
        testSuite.setWaitForTestCase(true);
        testSuite.setDurationSeconds(0);
        testSuite.setWarmupDurationSeconds(1);
        parallel = true;
        verifyEnabled = false;

        RunTestSuiteTask task = createRunTestSuiteTask();
        task.run();

        verifyRemoteClient();
    }

    @Test
    public void runTestSuiteSequential_withSingleTest() {
        TestCase testCase = new TestCase("CoordinatorTest");

        testSuite = new TestSuite();
        testSuite.addTest(testCase);
        testSuite.setDurationSeconds(1);

        RunTestSuiteTask task = createRunTestSuiteTask();
        task.run();

        verifyRemoteClient();
    }

    @Test
    public void runTestSuiteParallel_withSingleTest() {
        TestCase testCase = new TestCase("CoordinatorTest");

        testSuite = new TestSuite();
        testSuite.addTest(testCase);
        testSuite.setDurationSeconds(1);

        parallel = true;

        RunTestSuiteTask task = createRunTestSuiteTask();
        task.run();

        verifyRemoteClient();
    }

    @Test
    public void runTestSuiteSequential_hasCriticalFailures() {
        testSuite.setDurationSeconds(4);
        parallel = false;

        RunTestSuiteTask task = createRunTestSuiteTask();
        failureCollector.notify(criticalFailureOperation);
        task.run();
    }

    @Test
    public void runTestSuiteParallel_hasCriticalFailures() {
        testSuite.setDurationSeconds(4);
        testSuite.setFailFast(false);
        parallel = true;

        RunTestSuiteTask task = createRunTestSuiteTask();
        failureCollector.notify(criticalFailureOperation);
        task.run();
    }

    @Test
    public void runTestSuiteSequential_hasCriticalFailures_withFailFast() {
        testSuite.setDurationSeconds(1);
        testSuite.setFailFast(true);

        RunTestSuiteTask task = createRunTestSuiteTask();
        failureCollector.notify(criticalFailureOperation);
        task.run();
    }

    @Test
    public void runTestSuiteParallel_hasCriticalFailures_withFailFast() {
        testSuite.setDurationSeconds(1);
        testSuite.setFailFast(true);
        parallel = true;

        RunTestSuiteTask task = createRunTestSuiteTask();
        failureCollector.notify(criticalFailureOperation);
        task.run();
    }

    @Test(expected = IllegalStateException.class)
    public void runTestSuiteSequential_withException() {
        doThrow(new IllegalStateException("expected")).when(remoteClient).sendToAllWorkers(any(SimulatorOperation.class));
        testSuite.setDurationSeconds(1);
        parallel = false;

        RunTestSuiteTask task = createRunTestSuiteTask();
        task.run();
    }

    @Test(expected = IllegalStateException.class)
    public void runTestSuiteParallel_withException() {
        doThrow(new IllegalStateException("expected")).when(remoteClient).sendToAllWorkers(any(SimulatorOperation.class));
        testSuite.setDurationSeconds(1);
        parallel = true;

        RunTestSuiteTask task = createRunTestSuiteTask();
        task.run();
    }

    @Test
    public void runTestSuiteSequential_withWorkerNotShuttingDown() {
        simulatorProperties.set("WAIT_FOR_WORKER_SHUTDOWN_TIMEOUT_SECONDS", "1");
        testSuite.setDurationSeconds(1);
        finishWorkerLatch = null;

        RunTestSuiteTask task = createRunTestSuiteTask();
        task.run();

        verifyRemoteClient();
    }

    private RunTestSuiteTask createRunTestSuiteTask() {
        return createRunTestSuiteTask(0);
    }

    private RunTestSuiteTask createRunTestSuiteTask(int targetCount) {
        WorkerProcessSettings workerProcessSettings = mock(WorkerProcessSettings.class);
        when(workerProcessSettings.getWorkerIndex()).thenReturn(1);

        componentRegistry = new ComponentRegistry();
        componentRegistry.addAgent("127.0.0.1", "127.0.0.1");
        componentRegistry.addWorkers(componentRegistry.getFirstAgent().getAddress(), singletonList(workerProcessSettings));
        componentRegistry.addTests(testSuite);

        failureCollector = new FailureCollector(outputDirectory, Collections.<FailureType>emptySet());
        PerformanceStatsCollector performanceStatsCollector = new PerformanceStatsCollector();
        TestPhaseListeners testPhaseListeners = new TestPhaseListeners();

        CoordinatorParameters coordinatorParameters = mock(CoordinatorParameters.class);
        when(coordinatorParameters.getSimulatorProperties()).thenReturn(simulatorProperties);
        when(coordinatorParameters.isVerifyEnabled()).thenReturn(verifyEnabled);
        when(coordinatorParameters.isParallel()).thenReturn(parallel);
        when(coordinatorParameters.getTargetType(anyBoolean())).thenReturn(TargetType.ALL);
        when(coordinatorParameters.getTargetCount()).thenReturn(targetCount);

        WorkerParameters workerParameters = mock(WorkerParameters.class);
        when(workerParameters.isMonitorPerformance()).thenReturn(monitorPerformance);
        when(workerParameters.getWorkerPerformanceMonitorIntervalSeconds()).thenReturn(3);
        when(workerParameters.getRunPhaseLogIntervalSeconds(anyInt())).thenReturn(3);

        RunTestSuiteTask task = new RunTestSuiteTask(testSuite, coordinatorParameters, componentRegistry, failureCollector,
                testPhaseListeners, remoteClient, performanceStatsCollector,
                workerParameters);

        new TestPhaseCompleter(componentRegistry, testPhaseListeners, failureCollector).start();

        return task;
    }

    private void verifyRemoteClient() {
        int numberOfTests = testSuite.size();
        boolean isStopTestOperation = (testSuite.getDurationSeconds() > 0);
        List<TestPhase> expectedTestPhases = getExpectedTestPhases();

        // calculate how many remote calls we expect:
        // - StartTestOperations
        // - StopTestOperations
        // - StarTestPhaseOperation the first worker
        // - StarTestPhaseOperation to all workers
        int expectedStartTest = 0;
        int expectedStartTestPhaseOnFirstWorker = 0;
        int expectedStartTestPhaseOnAllWorkers = 0;
        // increase expected counters for each TestPhase
        for (TestPhase testPhase : expectedTestPhases) {
            if (testPhase == WARMUP || testPhase == RUN) {
                expectedStartTest++;
            } else if (testPhase.isGlobal()) {
                expectedStartTestPhaseOnFirstWorker++;
            } else {
                expectedStartTestPhaseOnAllWorkers++;
            }
        }
        int expectedStopTest = (isStopTestOperation ? expectedStartTest : 0);

        // verify number of remote calls
        ArgumentCaptor<SimulatorOperation> argumentCaptor = ArgumentCaptor.forClass(SimulatorOperation.class);
        verify(remoteClient, times(numberOfTests)).sendToAllWorkers(any(CreateTestOperation.class));
        int expectedTimes = numberOfTests * expectedStartTestPhaseOnFirstWorker;
        verify(remoteClient, times(expectedTimes)).sendToTestOnFirstWorker(anyString(), any(StartTestPhaseOperation.class));
        expectedTimes = numberOfTests * (expectedStartTestPhaseOnAllWorkers + expectedStartTest + expectedStopTest);
        verify(remoteClient, times(expectedTimes)).sendToTestOnAllWorkers(anyString(), argumentCaptor.capture());
        verify(remoteClient, atLeastOnce()).logOnAllAgents(anyString());

        // assert captured arguments
        int actualStartTestOperations = 0;
        int actualStopTestOperation = 0;
        int actualStartTestPhaseOperations = 0;
        for (SimulatorOperation operation : argumentCaptor.getAllValues()) {
            if (operation instanceof StartTestOperation) {
                actualStartTestOperations++;
            } else if (operation instanceof StopTestOperation) {
                actualStopTestOperation++;
            } else if (operation instanceof StartTestPhaseOperation) {
                actualStartTestPhaseOperations++;
                TestPhase actual = ((StartTestPhaseOperation) operation).getTestPhase();
                assertTrue(format("expected TestPhases should contain %s, but where %s", actual, expectedTestPhases),
                        expectedTestPhases.contains(actual));
            } else {
                fail("Unwanted SimulatorOperation: " + operation.getClass().getSimpleName());
            }
        }
        assertEquals(expectedStartTest * numberOfTests, actualStartTestOperations);
        assertEquals(expectedStartTestPhaseOnAllWorkers * numberOfTests, actualStartTestPhaseOperations);
        if (isStopTestOperation) {
            assertEquals(expectedStopTest * numberOfTests, actualStopTestOperation);
        }
    }

    private List<TestPhase> getExpectedTestPhases() {
        // per default we expected all test phases to be called
        List<TestPhase> expectedTestPhases = new ArrayList<TestPhase>(asList(TestPhase.values()));
        if (!verifyEnabled) {
            // exclude verify test phases
            expectedTestPhases.remove(TestPhase.GLOBAL_VERIFY);
            expectedTestPhases.remove(TestPhase.LOCAL_VERIFY);
        }
        if (testSuite.getWarmupDurationSeconds() == 0) {
            // exclude warmup test phases
            expectedTestPhases.remove(WARMUP);
            expectedTestPhases.remove(TestPhase.LOCAL_AFTER_WARMUP);
            expectedTestPhases.remove(TestPhase.GLOBAL_AFTER_WARMUP);
        }
        return expectedTestPhases;
    }

    private class TestPhaseCompleter extends Thread {

        private final ComponentRegistry componentRegistry;
        private final TestPhaseListeners testPhaseListeners;
        private final FailureCollector failureCollector;

        private TestPhaseCompleter(ComponentRegistry componentRegistry, TestPhaseListeners testPhaseListeners,
                                   FailureCollector failureCollector) {
            super("TestPhaseCompleter");

            this.componentRegistry = componentRegistry;
            this.testPhaseListeners = testPhaseListeners;
            this.failureCollector = failureCollector;

            setDaemon(true);
        }

        @Override
        public void run() {
            SimulatorAddress workerAddress = new SimulatorAddress(WORKER, 1, 1, 0);

            for (TestPhase testPhase : TestPhase.values()) {
                sleepMillis(100);
                for (TestData testData : componentRegistry.getTests()) {
                    testPhaseListeners.updatePhaseCompletion(testData.getTestIndex(), testPhase, workerAddress);
                }
            }

            if (finishWorkerLatch != null) {
                await(finishWorkerLatch);
                FailureOperation operation = new FailureOperation("Worker finished", WORKER_FINISHED, workerAddress, "127.0.0.1",
                        "127.0.0.1:5701", "workerId", "testId", testSuite, "stacktrace");
                failureCollector.notify(operation);
            }
        }
    }
}
