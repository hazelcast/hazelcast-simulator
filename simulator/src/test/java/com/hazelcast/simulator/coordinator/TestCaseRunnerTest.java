package com.hazelcast.simulator.coordinator;

import com.hazelcast.simulator.agent.workerprocess.WorkerProcessSettings;
import com.hazelcast.simulator.common.AgentsFile;
import com.hazelcast.simulator.common.SimulatorProperties;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.operation.FailureOperation;
import com.hazelcast.simulator.protocol.operation.SimulatorOperation;
import com.hazelcast.simulator.protocol.registry.ComponentRegistry;
import com.hazelcast.simulator.protocol.registry.TargetType;
import com.hazelcast.simulator.protocol.registry.TestData;
import com.hazelcast.simulator.test.TestCase;
import com.hazelcast.simulator.test.TestPhase;
import com.hazelcast.simulator.test.TestSuite;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.mockito.verification.VerificationMode;

import java.io.File;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import static com.hazelcast.simulator.TestEnvironmentUtils.resetUserDir;
import static com.hazelcast.simulator.TestEnvironmentUtils.setDistributionUserDir;
import static com.hazelcast.simulator.protocol.core.AddressLevel.WORKER;
import static com.hazelcast.simulator.test.FailureType.WORKER_EXCEPTION;
import static com.hazelcast.simulator.test.FailureType.WORKER_FINISHED;
import static com.hazelcast.simulator.utils.CommonUtils.await;
import static com.hazelcast.simulator.utils.CommonUtils.sleepMillis;
import static com.hazelcast.simulator.utils.FileUtils.deleteQuiet;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestCaseRunnerTest {

    private CountDownLatch finishWorkerLatch = new CountDownLatch(1);

    private TestSuite testSuite;
    private FailureOperation criticalFailureOperation;

    private SimulatorProperties simulatorProperties;
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

        testSuite = new TestSuite("testrun-" + System.currentTimeMillis());
        testSuite.addTest(testCase1);
        testSuite.addTest(testCase2);

        SimulatorAddress address = new SimulatorAddress(WORKER, 1, 1, 0);
        criticalFailureOperation = new FailureOperation("expected critical failure", WORKER_EXCEPTION, address, "127.0.0.1",
                "127.0.0.1:5701", "workerId", "CoordinatorTest1", testSuite, "stacktrace");

        simulatorProperties = new SimulatorProperties();

        remoteClient = mock(RemoteClient.class);
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                if (finishWorkerLatch != null) {
                    finishWorkerLatch.countDown();
                }
                return null;
            }
        }).when(remoteClient).terminateWorkers(anyBoolean());
    }

    @After
    public void cleanUp() {
        deleteQuiet(new File(testSuite.getId()).getAbsoluteFile());
        deleteQuiet(AgentsFile.NAME);
    }

    @Test
    public void runTestSuiteParallel_waitForTestCase_and_duration() {
        testSuite.setWaitForTestCase(true);
        testSuite.setDurationSeconds(3);
        parallel = true;

        Coordinator coordinator = createCoordinator();
        coordinator.runTestSuite();

        verifyRemoteClient(coordinator);
    }

    @Test
    public void runTestSuiteParallel_waitForTestCase_noVerify() {
        testSuite.setWaitForTestCase(true);
        testSuite.setDurationSeconds(0);
        parallel = true;
        verifyEnabled = false;

        Coordinator coordinator = createCoordinator();
        coordinator.runTestSuite();

        verifyRemoteClient(coordinator);
    }

    @Test
    public void runTestSuiteParallel_performanceMonitorEnabled() {
        testSuite.setDurationSeconds(4);
        parallel = true;
        monitorPerformance = true;

        Coordinator coordinator = createCoordinator();
        coordinator.runTestSuite();

        verifyRemoteClient(coordinator);
    }

    @Test
    public void runTestSuiteParallel_withTargetCount() {
        testSuite.setWaitForTestCase(true);
        testSuite.setDurationSeconds(0);
        parallel = true;
        verifyEnabled = false;

        Coordinator coordinator = createCoordinator(1);
        coordinator.runTestSuite();

        verifyRemoteClient(coordinator);
    }

    @Test
    public void runTestSuiteSequential_withSingleTest() {
        TestCase testCase = new TestCase("CoordinatorTest");

        testSuite = new TestSuite();
        testSuite.addTest(testCase);
        testSuite.setDurationSeconds(1);

        Coordinator coordinator = createCoordinator();
        coordinator.runTestSuite();

        verifyRemoteClient(coordinator);
    }

    @Test
    public void runTestSuiteParallel_withSingleTest() {
        TestCase testCase = new TestCase("CoordinatorTest");

        testSuite = new TestSuite();
        testSuite.addTest(testCase);
        testSuite.setDurationSeconds(1);

        parallel = true;

        Coordinator coordinator = createCoordinator();
        coordinator.runTestSuite();

        verifyRemoteClient(coordinator);
    }

    @Test
    public void runTestSuiteSequential_hasCriticalFailures() {
        testSuite.setDurationSeconds(4);
        parallel = false;

        Coordinator coordinator = createCoordinator();
        coordinator.getFailureContainer().addFailureOperation(criticalFailureOperation);
        coordinator.runTestSuite();

        verifyRemoteClient(coordinator, true);
    }

    @Test
    public void runTestSuiteParallel_hasCriticalFailures() {
        testSuite.setDurationSeconds(4);
        testSuite.setFailFast(false);
        parallel = true;

        Coordinator coordinator = createCoordinator();
        coordinator.getFailureContainer().addFailureOperation(criticalFailureOperation);
        coordinator.runTestSuite();

        verifyRemoteClient(coordinator, true);
    }

    @Test
    public void runTestSuiteSequential_hasCriticalFailures_withFailFast() {
        testSuite.setDurationSeconds(1);
        testSuite.setFailFast(true);

        Coordinator coordinator = createCoordinator();
        coordinator.getFailureContainer().addFailureOperation(criticalFailureOperation);
        coordinator.runTestSuite();

        verifyRemoteClient(coordinator, true);
    }

    @Test
    public void runTestSuiteParallel_hasCriticalFailures_withFailFast() {
        testSuite.setDurationSeconds(1);
        testSuite.setFailFast(true);
        parallel = true;

        Coordinator coordinator = createCoordinator();
        coordinator.getFailureContainer().addFailureOperation(criticalFailureOperation);
        coordinator.runTestSuite();

        verifyRemoteClient(coordinator, true);
    }

    @Test(expected = IllegalStateException.class)
    public void runTestSuiteSequential_withException() {
        doThrow(new IllegalStateException("expected")).when(remoteClient).sendToAllWorkers(any(SimulatorOperation.class));
        testSuite.setDurationSeconds(1);
        parallel = false;

        Coordinator coordinator = createCoordinator();
        coordinator.runTestSuite();
    }

    @Test(expected = IllegalStateException.class)
    public void runTestSuiteParallel_withException() {
        doThrow(new IllegalStateException("expected")).when(remoteClient).sendToAllWorkers(any(SimulatorOperation.class));
        testSuite.setDurationSeconds(1);
        parallel = true;

        Coordinator coordinator = createCoordinator();
        coordinator.runTestSuite();
    }

    @Test
    public void runTestSuiteSequential_withWorkerNotShuttingDown() {
        simulatorProperties.set("WAIT_FOR_WORKER_SHUTDOWN_TIMEOUT_SECONDS", "1");
        testSuite.setDurationSeconds(1);
        finishWorkerLatch = null;

        Coordinator coordinator = createCoordinator();
        coordinator.runTestSuite();

        Set<SimulatorAddress> finishedWorkers = coordinator.getFailureContainer().getFinishedWorkers();
        assertEquals(0, finishedWorkers.size());

        Set<SimulatorAddress> missingWorkers = coordinator.getComponentRegistry().getMissingWorkers(finishedWorkers);
        assertEquals(1, missingWorkers.size());

        verifyRemoteClient(coordinator);
    }

    private Coordinator createCoordinator() {
        return createCoordinator(0);
    }

    private Coordinator createCoordinator(int targetCount) {
        WorkerProcessSettings workerProcessSettings = mock(WorkerProcessSettings.class);
        when(workerProcessSettings.getWorkerIndex()).thenReturn(1);

        ComponentRegistry componentRegistry = new ComponentRegistry();
        componentRegistry.addAgent("127.0.0.1", "127.0.0.1");
        componentRegistry.addWorkers(componentRegistry.getFirstAgent().getAddress(), singletonList(workerProcessSettings));
        componentRegistry.addTests(testSuite);

        CoordinatorParameters coordinatorParameters = mock(CoordinatorParameters.class);
        when(coordinatorParameters.getSimulatorProperties()).thenReturn(simulatorProperties);
        when(coordinatorParameters.isVerifyEnabled()).thenReturn(verifyEnabled);
        when(coordinatorParameters.isParallel()).thenReturn(parallel);
        when(coordinatorParameters.isRefreshJvm()).thenReturn(false);
        when(coordinatorParameters.getTargetType(anyBoolean())).thenReturn(TargetType.ALL);
        when(coordinatorParameters.getTargetCount()).thenReturn(targetCount);

        ClusterLayoutParameters clusterLayoutParameters = mock(ClusterLayoutParameters.class);
        when(clusterLayoutParameters.getDedicatedMemberMachineCount()).thenReturn(0);
        when(clusterLayoutParameters.getMemberWorkerCount()).thenReturn(1);
        when(clusterLayoutParameters.getClientWorkerCount()).thenReturn(0);

        WorkerParameters workerParameters = mock(WorkerParameters.class);
        when(workerParameters.isMonitorPerformance()).thenReturn(monitorPerformance);
        when(workerParameters.getWorkerPerformanceMonitorIntervalSeconds()).thenReturn(3);
        when(workerParameters.getRunPhaseLogIntervalSeconds(anyInt())).thenReturn(3);

        Coordinator coordinator = new Coordinator(testSuite, componentRegistry, coordinatorParameters, workerParameters,
                clusterLayoutParameters);
        coordinator.setRemoteClient(remoteClient);

        new TestPhaseCompleter(coordinator).start();

        return coordinator;
    }

    private void verifyRemoteClient(Coordinator coordinator) {
        verifyRemoteClient(coordinator, false);
    }

    private void verifyRemoteClient(Coordinator coordinator, boolean hasCriticalFailures) {
        boolean verifyExecuteOnAllWorkersWithRange = false;
        int numberOfTests = testSuite.size();
        int createTestCount = numberOfTests;
        // there are no default operations sent to the first Worker
        int sendToTestOnFirstWorkerTimes = 0;
        // there are default operations sent to all Workers: StopTestOperation
        int sendToTestOnAllWorkersTimes = 1;
        // increase expected counters for each TestPhase
        for (TestPhase testPhase : TestPhase.values()) {
            if (testPhase.isGlobal()) {
                sendToTestOnFirstWorkerTimes++;
            } else {
                sendToTestOnAllWorkersTimes++;
            }
        }
        if (!coordinator.getCoordinatorParameters().isVerifyEnabled()) {
            // no StartTestPhaseOperation for global and local verify phase are sent
            sendToTestOnFirstWorkerTimes--;
            sendToTestOnAllWorkersTimes--;
        }
        if (testSuite.getDurationSeconds() == 0) {
            // no StopTestOperation is sent
            sendToTestOnAllWorkersTimes--;
        } else if (testSuite.isWaitForTestCase()) {
            // has duration and waitForTestCase
            verifyExecuteOnAllWorkersWithRange = true;
        }
        if (hasCriticalFailures) {
            // adjust expected counters if test has critical failures
            verifyExecuteOnAllWorkersWithRange = true;
            if (testSuite.isFailFast()) {
                if (!parallel) {
                    createTestCount = 1;
                }
                sendToTestOnFirstWorkerTimes = 1;
                sendToTestOnAllWorkersTimes = 1;
            } else {
                sendToTestOnFirstWorkerTimes = 2;
                sendToTestOnAllWorkersTimes = 4;
            }
        }

        verify(remoteClient, times(createTestCount)).sendToAllWorkers(any(SimulatorOperation.class));
        if (verifyExecuteOnAllWorkersWithRange) {
            VerificationMode atLeast = atLeast((sendToTestOnAllWorkersTimes - 1) * numberOfTests);
            VerificationMode atMost = atMost(sendToTestOnAllWorkersTimes * numberOfTests);
            verify(remoteClient, atLeast).sendToTestOnAllWorkers(anyString(), any(SimulatorOperation.class));
            verify(remoteClient, atMost).sendToTestOnAllWorkers(anyString(), any(SimulatorOperation.class));

            atLeast = atLeast((sendToTestOnFirstWorkerTimes - 1) * numberOfTests);
            atMost = atMost(sendToTestOnFirstWorkerTimes * numberOfTests);
            verify(remoteClient, atLeast).sendToTestOnFirstWorker(anyString(), any(SimulatorOperation.class));
            verify(remoteClient, atMost).sendToTestOnFirstWorker(anyString(), any(SimulatorOperation.class));
        } else {
            VerificationMode times = times(sendToTestOnAllWorkersTimes * numberOfTests);
            verify(remoteClient, times).sendToTestOnAllWorkers(anyString(), any(SimulatorOperation.class));

            times = times(sendToTestOnFirstWorkerTimes * numberOfTests);
            verify(remoteClient, times).sendToTestOnFirstWorker(anyString(), any(SimulatorOperation.class));
        }
        verify(remoteClient, times(1)).terminateWorkers(true);
        verify(remoteClient, atLeastOnce()).logOnAllAgents(anyString());
    }

    private class TestPhaseCompleter extends Thread {

        private final ComponentRegistry componentRegistry;
        private final TestPhaseListeners testPhaseListeners;
        private final FailureContainer failureContainer;

        private TestPhaseCompleter(Coordinator coordinator) {
            super("TestPhaseCompleter");

            this.componentRegistry = coordinator.getComponentRegistry();
            this.testPhaseListeners = coordinator.getTestPhaseListeners();
            this.failureContainer = coordinator.getFailureContainer();

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
                failureContainer.addFailureOperation(operation);
            }
        }
    }
}
