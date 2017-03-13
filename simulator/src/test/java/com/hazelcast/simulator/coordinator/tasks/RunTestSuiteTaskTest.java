package com.hazelcast.simulator.coordinator.tasks;

import com.hazelcast.simulator.agent.workerprocess.WorkerProcessSettings;
import com.hazelcast.simulator.common.SimulatorProperties;
import com.hazelcast.simulator.common.TestCase;
import com.hazelcast.simulator.common.TestPhase;
import com.hazelcast.simulator.common.WorkerType;
import com.hazelcast.simulator.coordinator.CoordinatorParameters;
import com.hazelcast.simulator.coordinator.FailureCollector;
import com.hazelcast.simulator.coordinator.PerformanceStatsCollector;
import com.hazelcast.simulator.coordinator.RemoteClient;
import com.hazelcast.simulator.coordinator.TestPhaseListeners;
import com.hazelcast.simulator.coordinator.TestSuite;
import com.hazelcast.simulator.protocol.connector.Connector;
import com.hazelcast.simulator.protocol.core.Response;
import com.hazelcast.simulator.protocol.core.ResponseType;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.operation.CreateTestOperation;
import com.hazelcast.simulator.protocol.operation.FailureOperation;
import com.hazelcast.simulator.protocol.operation.SimulatorOperation;
import com.hazelcast.simulator.protocol.operation.StartTestOperation;
import com.hazelcast.simulator.protocol.operation.StartTestPhaseOperation;
import com.hazelcast.simulator.protocol.operation.StopTestOperation;
import com.hazelcast.simulator.coordinator.registry.ComponentRegistry;
import com.hazelcast.simulator.coordinator.registry.TargetType;
import com.hazelcast.simulator.coordinator.registry.TestData;
import com.hazelcast.simulator.coordinator.registry.WorkerQuery;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.simulator.TestEnvironmentUtils.setupFakeEnvironment;
import static com.hazelcast.simulator.TestEnvironmentUtils.tearDownFakeEnvironment;
import static com.hazelcast.simulator.common.FailureType.WORKER_EXCEPTION;
import static com.hazelcast.simulator.common.FailureType.WORKER_NORMAL_EXIT;
import static com.hazelcast.simulator.common.TestPhase.RUN;
import static com.hazelcast.simulator.coordinator.tasks.RunTestSuiteTask.getTestPhaseSyncMap;
import static com.hazelcast.simulator.protocol.core.AddressLevel.WORKER;
import static com.hazelcast.simulator.utils.CommonUtils.await;
import static com.hazelcast.simulator.utils.CommonUtils.sleepMillis;
import static com.hazelcast.simulator.utils.FileUtils.deleteQuiet;
import static com.hazelcast.simulator.utils.TestUtils.createTmpDirectory;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RunTestSuiteTaskTest {

    private static final AtomicLong ID_GENERATOR = new AtomicLong();
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
    private int monitorPerformanceMonitorIntervalSeconds = 0;

    @BeforeClass
    public static void prepareEnvironment() {
        setupFakeEnvironment();
    }

    @AfterClass
    public static void resetEnvironment() {
        tearDownFakeEnvironment();
    }

    @Before
    public void before() {
        testSuite = new TestSuite();
        testSuite.addTest(new TestCase("CoordinatorTest" + ID_GENERATOR.incrementAndGet()));
        testSuite.addTest(new TestCase("CoordinatorTest" + ID_GENERATOR.incrementAndGet()));

        outputDirectory = createTmpDirectory();

        SimulatorAddress address = new SimulatorAddress(WORKER, 1, 1, 0);
        criticalFailureOperation = new FailureOperation("expected critical failure", WORKER_EXCEPTION, address, "127.0.0.1",
                "127.0.0.1:5701", "workerId", "CoordinatorTest1", "stacktrace");

        simulatorProperties = new SimulatorProperties();

        Response response = new Response(1, SimulatorAddress.COORDINATOR, address, ResponseType.SUCCESS);

        Connector connector = mock(Connector.class);
        when(connector.invoke(any(SimulatorAddress.class), any(SimulatorOperation.class))).thenReturn(response);

        remoteClient = mock(RemoteClient.class);
        when(remoteClient.getConnector()).thenReturn(connector);
    }

    @After
    public void cleanUp() {
        deleteQuiet(outputDirectory);
    }

    @Test
    public void runParallel_waitForTestCase_and_duration() {
        testSuite.setDurationSeconds(3);
        parallel = true;

        RunTestSuiteTask task = createRunTestSuiteTask();
        task.run();

        verifyRemoteClient();
    }

    @Test
    public void runParallel_waitForTestCase_noVerify() {
        testSuite.setDurationSeconds(0);
        parallel = true;
        verifyEnabled = false;

        RunTestSuiteTask task = createRunTestSuiteTask();
        task.run();

        verifyRemoteClient();
    }

    @Test
    public void runParallel_performanceMonitorEnabled() {
        testSuite.setDurationSeconds(4);
        parallel = true;
        monitorPerformanceMonitorIntervalSeconds = 10;

        RunTestSuiteTask task = createRunTestSuiteTask();
        task.run();

        verifyRemoteClient();
    }

    @Test
    public void runParallel_withTargetCount() {
        testSuite.setDurationSeconds(0);
        parallel = true;
        verifyEnabled = false;

        RunTestSuiteTask task = createRunTestSuiteTask(1);
        task.run();

        verifyRemoteClient();
    }

    @Test
    public void runParallel_withWarmup() {
        testSuite.setDurationSeconds(1);
        parallel = true;
        verifyEnabled = false;

        RunTestSuiteTask task = createRunTestSuiteTask();
        task.run();

        verifyRemoteClient();
    }

    @Test
    public void runParallel_withWarmup_waitForTestCase() {
        testSuite.setDurationSeconds(0);
        parallel = true;
        verifyEnabled = false;

        RunTestSuiteTask task = createRunTestSuiteTask();
        task.run();

        verifyRemoteClient();
    }

    @Test
    public void runSequential_withSingleTest() {
        TestCase testCase = new TestCase("CoordinatorTest" + ID_GENERATOR.incrementAndGet());

        testSuite = new TestSuite();
        testSuite.addTest(testCase);
        testSuite.setDurationSeconds(1);

        RunTestSuiteTask task = createRunTestSuiteTask();
        task.run();

        verifyRemoteClient();
    }

    @Test
    public void runParallel_withSingleTest() {
        TestCase testCase = new TestCase("CoordinatorTest" + ID_GENERATOR.incrementAndGet());

        testSuite = new TestSuite();
        testSuite.addTest(testCase);
        testSuite.setDurationSeconds(1);

        parallel = true;

        RunTestSuiteTask task = createRunTestSuiteTask();
        task.run();

        verifyRemoteClient();
    }

    @Test
    public void runSequential_hasCriticalFailures() {
        testSuite.setDurationSeconds(4);
        parallel = false;

        RunTestSuiteTask task = createRunTestSuiteTask();
        failureCollector.notify(criticalFailureOperation);
        task.run();
    }

    @Test
    public void runParallel_hasCriticalFailures() {
        testSuite.setDurationSeconds(4);
        testSuite.setFailFast(false);
        parallel = true;

        RunTestSuiteTask task = createRunTestSuiteTask();
        failureCollector.notify(criticalFailureOperation);
        task.run();
    }

    @Test
    public void runSequential_hasCriticalFailures_withFailFast() {
        testSuite.setDurationSeconds(1);
        testSuite.setFailFast(true);

        RunTestSuiteTask task = createRunTestSuiteTask();
        failureCollector.notify(criticalFailureOperation);
        task.run();
    }

    @Test
    public void runParallel_hasCriticalFailures_withFailFast() {
        testSuite.setDurationSeconds(1);
        testSuite.setFailFast(true);
        parallel = true;

        RunTestSuiteTask task = createRunTestSuiteTask();
        failureCollector.notify(criticalFailureOperation);
        task.run();
    }

    @Test(expected = IllegalStateException.class)
    public void runSequential_withException() {
        doThrow(new IllegalStateException("expected")).when(remoteClient).invokeOnAllWorkers(any(SimulatorOperation.class));
        testSuite.setDurationSeconds(1);
        parallel = false;

        RunTestSuiteTask task = createRunTestSuiteTask();
        task.run();
    }

    @Test(expected = IllegalStateException.class)
    public void runParallel_withException() {
        doThrow(new IllegalStateException("expected")).when(remoteClient).invokeOnAllWorkers(any(SimulatorOperation.class));
        testSuite.setDurationSeconds(1);
        parallel = true;

        RunTestSuiteTask task = createRunTestSuiteTask();
        task.run();
    }

    @Test
    public void runSequential_withWorkerNotShuttingDown() {
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
        when(workerProcessSettings.getWorkerType()).thenReturn(WorkerType.MEMBER);

        componentRegistry = new ComponentRegistry();
        componentRegistry.addAgent("127.0.0.1", "127.0.0.1");
        componentRegistry.addWorkers(componentRegistry.getFirstAgent().getAddress(), singletonList(workerProcessSettings));

        failureCollector = new FailureCollector(outputDirectory, componentRegistry);
        PerformanceStatsCollector performanceStatsCollector = new PerformanceStatsCollector();
        TestPhaseListeners testPhaseListeners = new TestPhaseListeners();

        CoordinatorParameters coordinatorParameters = new CoordinatorParameters()
                .setPerformanceMonitorIntervalSeconds(monitorPerformanceMonitorIntervalSeconds)
                .setSimulatorProperties(simulatorProperties);

        WorkerQuery query = new WorkerQuery().setTargetType(TargetType.ALL);
        if (targetCount > 0) {
            query.setMaxCount(targetCount);
        }
        testSuite.setVerifyEnabled(verifyEnabled)
                .setParallel(parallel)
                .setWorkerQuery(query);

        RunTestSuiteTask task = new RunTestSuiteTask(testSuite, coordinatorParameters, componentRegistry, failureCollector,
                testPhaseListeners, remoteClient, performanceStatsCollector);

        new TestPhaseCompleter(componentRegistry, testPhaseListeners, failureCollector).start();

        return task;
    }

    private void verifyRemoteClient() {
        int testCount = testSuite.size();
        List<TestPhase> expectedTestPhases = getExpectedTestPhases();

        // in the remainingPhaseCount the remaining number of calls per phase. Eventually everything should be 0.
        Map<TestPhase, AtomicInteger> remainingPhaseCount = new HashMap<TestPhase, AtomicInteger>();
        for (TestPhase testPhase : expectedTestPhases) {
            remainingPhaseCount.put(testPhase, new AtomicInteger(testCount));
        }

        // we check if the create calls have been made
        verify(remoteClient, times(testCount)).invokeOnAllWorkers(any(CreateTestOperation.class));

        // now we suck up all 'invokeOnTestOnAllWorkers'
        ArgumentCaptor<SimulatorOperation> allTestOperations = ArgumentCaptor.forClass(SimulatorOperation.class);
        verify(remoteClient, atLeast(0)).invokeOnTestOnAllWorkers(any(SimulatorAddress.class), allTestOperations.capture());

        // now we suck up all 'invokeOnTestOnFirstWorker'
        ArgumentCaptor<SimulatorOperation> firstTestOperations = ArgumentCaptor.forClass(SimulatorOperation.class);
        verify(remoteClient, atLeast(0)).invokeOnTestOnFirstWorker(any(SimulatorAddress.class), firstTestOperations.capture());

        int actualStopTestCount = 0;

        //
        for (SimulatorOperation operation : allTestOperations.getAllValues()) {
            if (operation instanceof StartTestPhaseOperation) {
                remainingPhaseCount.get(((StartTestPhaseOperation) operation).getTestPhase()).decrementAndGet();
            } else if (operation instanceof StartTestOperation) {
                TestPhase phase =  RUN;
                remainingPhaseCount.get(phase).decrementAndGet();
            } else if (operation instanceof StopTestOperation) {
                actualStopTestCount++;
            } else {
                fail("Unrecognized operation: " + operation);
            }
        }

        for (SimulatorOperation operation : firstTestOperations.getAllValues()) {
            if (operation instanceof StartTestPhaseOperation) {
                remainingPhaseCount.get(((StartTestPhaseOperation) operation).getTestPhase()).decrementAndGet();
            } else {
                fail("Unrecognized operation: " + operation);
            }
        }

        int expectedStopCount = testCount;
        assertEquals("actualStopTestCount incorrect", expectedStopCount, actualStopTestCount);

        for (Map.Entry<TestPhase, AtomicInteger> entry : remainingPhaseCount.entrySet()) {
            TestPhase phase = entry.getKey();
            int value = entry.getValue().get();
            assertEquals("Number of remaining occurrences for phase: " + phase + " incorrect", 0, value);
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
                    testPhaseListeners.onCompletion(testData.getTestIndex(), testPhase, workerAddress);
                }
            }

            if (finishWorkerLatch != null) {
                await(finishWorkerLatch);
                FailureOperation operation = new FailureOperation("Worker finished", WORKER_NORMAL_EXIT, workerAddress, "127.0.0.1",
                        "127.0.0.1:5701", "workerId", "testId", "stacktrace");
                failureCollector.notify(operation);
            }
        }
    }

    @Test
    public void testGetTestPhaseSyncMap() {
        Map<TestPhase, CountDownLatch> testPhaseSyncMap = getTestPhaseSyncMap(5, true, TestPhase.RUN);

        assertEquals(5, testPhaseSyncMap.get(TestPhase.SETUP).getCount());
        assertEquals(5, testPhaseSyncMap.get(TestPhase.LOCAL_PREPARE).getCount());
        assertEquals(5, testPhaseSyncMap.get(TestPhase.GLOBAL_PREPARE).getCount());
        assertEquals(5, testPhaseSyncMap.get(TestPhase.RUN).getCount());
        assertEquals(0, testPhaseSyncMap.get(TestPhase.GLOBAL_VERIFY).getCount());
        assertEquals(0, testPhaseSyncMap.get(TestPhase.LOCAL_VERIFY).getCount());
        assertEquals(0, testPhaseSyncMap.get(TestPhase.GLOBAL_TEARDOWN).getCount());
        assertEquals(0, testPhaseSyncMap.get(TestPhase.LOCAL_TEARDOWN).getCount());
    }

    @Test
    @SuppressWarnings("all")
    public void testGetTestPhaseSyncMap_notParallel() {
        Map<TestPhase, CountDownLatch> testPhaseSyncMap = getTestPhaseSyncMap(5, false, TestPhase.RUN);

        assertNull(testPhaseSyncMap);
    }
}
