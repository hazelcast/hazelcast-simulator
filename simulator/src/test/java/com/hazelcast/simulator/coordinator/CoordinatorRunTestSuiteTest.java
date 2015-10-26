package com.hazelcast.simulator.coordinator;

import com.hazelcast.simulator.agent.workerjvm.WorkerJvmSettings;
import com.hazelcast.simulator.common.AgentsFile;
import com.hazelcast.simulator.common.SimulatorProperties;
import com.hazelcast.simulator.protocol.core.AddressLevel;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.operation.FailureOperation;
import com.hazelcast.simulator.protocol.operation.SimulatorOperation;
import com.hazelcast.simulator.protocol.registry.ComponentRegistry;
import com.hazelcast.simulator.test.TestCase;
import com.hazelcast.simulator.test.TestPhase;
import com.hazelcast.simulator.test.TestSuite;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.Collections;

import static com.hazelcast.simulator.TestEnvironmentUtils.resetUserDir;
import static com.hazelcast.simulator.TestEnvironmentUtils.setDistributionUserDir;
import static com.hazelcast.simulator.test.FailureType.NETTY_EXCEPTION;
import static com.hazelcast.simulator.test.FailureType.WORKER_EXCEPTION;
import static com.hazelcast.simulator.test.FailureType.WORKER_FINISHED;
import static com.hazelcast.simulator.utils.CommonUtils.sleepMillis;
import static com.hazelcast.simulator.utils.FileUtils.deleteQuiet;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CoordinatorRunTestSuiteTest {

    private TestSuite testSuite;
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

        testSuite = new TestSuite();
        testSuite.addTest(testCase1);
        testSuite.addTest(testCase2);

        simulatorProperties = new SimulatorProperties();

        remoteClient = mock(RemoteClient.class);
    }

    @After
    public void cleanUp() {
        deleteQuiet(new File(AgentsFile.NAME));
        deleteQuiet(new File("failures-" + testSuite.getId() + ".txt"));
        deleteQuiet(new File("probes-" + testSuite.getId() + "_CoordinatorTest1.xml"));
        deleteQuiet(new File("probes-" + testSuite.getId() + "_CoordinatorTest2.xml"));
    }

    @Test
    public void runTestSuiteParallel_waitForTestCase_and_duration() throws Exception {
        testSuite.setWaitForTestCase(true);
        testSuite.setDurationSeconds(3);
        parallel = true;

        Coordinator coordinator = createCoordinator();
        coordinator.runTestSuite();

        verifyRemoteClient(coordinator);
    }

    @Test
    public void runTestSuiteParallel_waitForTestCase_noVerify() throws Exception {
        testSuite.setWaitForTestCase(true);
        testSuite.setDurationSeconds(0);
        parallel = true;
        verifyEnabled = false;

        Coordinator coordinator = createCoordinator();
        coordinator.runTestSuite();

        verifyRemoteClient(coordinator);
    }

    @Test
    public void runTestSuiteParallel_performanceMonitorEnabled() throws Exception {
        testSuite.setDurationSeconds(4);
        parallel = true;
        monitorPerformance = true;

        Coordinator coordinator = createCoordinator();
        coordinator.runTestSuite();

        verifyRemoteClient(coordinator);
    }

    @Test
    public void runTestSuiteSequential_hasCriticalFailures() throws Exception {
        testSuite.setDurationSeconds(4);
        parallel = false;

        Coordinator coordinator = createCoordinator();
        coordinator.getFailureContainer().addFailureOperation(
                new FailureOperation("expected critical failure", NETTY_EXCEPTION, null, "127.0.0.1", "127.0.0.1:5701",
                        "workerId", "testId", testSuite, "stacktrace")
        );
        coordinator.runTestSuite();

        verifyRemoteClient(coordinator);
    }

    @Test
    public void runTestSuiteParallel_hasCriticalFailures() throws Exception {
        testSuite.setDurationSeconds(4);
        parallel = true;

        Coordinator coordinator = createCoordinator();
        coordinator.getFailureContainer().addFailureOperation(
                new FailureOperation("expected critical failure", WORKER_EXCEPTION, null, "127.0.0.1", "127.0.0.1:5701",
                        "workerId", "CoordinatorTest1", testSuite, "stacktrace")
        );
        coordinator.runTestSuite();

        verifyRemoteClient(coordinator);
    }

    @Test(expected = IllegalStateException.class)
    public void runTestSuiteSequential_withException() throws Exception {
        doThrow(new IllegalStateException("expected")).when(remoteClient).sendToAllWorkers(any(SimulatorOperation.class));
        testSuite.setDurationSeconds(1);
        parallel = false;

        Coordinator coordinator = createCoordinator();
        coordinator.runTestSuite();
    }

    @Test(expected = IllegalStateException.class)
    public void runTestSuiteParallel_withException() throws Exception {
        doThrow(new IllegalStateException("expected")).when(remoteClient).sendToAllWorkers(any(SimulatorOperation.class));
        testSuite.setDurationSeconds(1);
        parallel = true;

        Coordinator coordinator = createCoordinator();
        coordinator.runTestSuite();
    }

    private Coordinator createCoordinator() {
        CoordinatorParameters coordinatorParameters = new CoordinatorParameters(
                simulatorProperties,
                new File(AgentsFile.NAME),
                "",
                verifyEnabled,
                parallel,
                TestPhase.SETUP,
                false
        );
        ClusterLayoutParameters clusterLayoutParameters = mock(ClusterLayoutParameters.class);

        WorkerParameters workerParameters = mock(WorkerParameters.class);
        when(workerParameters.isMonitorPerformance()).thenReturn(monitorPerformance);
        when(workerParameters.getWorkerPerformanceMonitorIntervalSeconds()).thenReturn(3);
        when(workerParameters.getRunPhaseLogIntervalSeconds(anyInt())).thenReturn(3);

        Coordinator coordinator = new Coordinator(coordinatorParameters, clusterLayoutParameters, workerParameters, testSuite);
        coordinator.setRemoteClient(remoteClient);

        WorkerJvmSettings workerJvmSettings = mock(WorkerJvmSettings.class);
        when(workerJvmSettings.getWorkerIndex()).thenReturn(1);

        ComponentRegistry componentRegistry = coordinator.getComponentRegistry();
        SimulatorAddress agentAddress = new SimulatorAddress(AddressLevel.AGENT, 1, 0, 0);
        componentRegistry.addWorkers(agentAddress, Collections.singletonList(workerJvmSettings));

        new TestPhaseCompleter(coordinator);

        return coordinator;
    }

    private void verifyRemoteClient(Coordinator coordinator) throws Exception {
        boolean verifyExecuteOnAllWorkersWithRange = false;
        int numberOfTests = testSuite.size();
        int executeOnFirstWorkerTimes = 0;
        int executeOnAllWorkersTimes = 2; // CreateTestOperation and StopTestOperation
        for (TestPhase testPhase : TestPhase.values()) {
            if (testPhase.desc().startsWith("global")) {
                executeOnFirstWorkerTimes++;
            } else {
                executeOnAllWorkersTimes++;
            }
        }
        if (testSuite.getDurationSeconds() == 0) {
            // no StopCommand is sent
            executeOnAllWorkersTimes--;
        } else if (testSuite.isWaitForTestCase()) {
            // has duration and waitForTestCase
            verifyExecuteOnAllWorkersWithRange = true;
        }
        if (!coordinator.getCoordinatorParameters().isVerifyEnabled()) {
            // no GenericCommand for global and local verify phase are sent
            executeOnFirstWorkerTimes--;
            executeOnAllWorkersTimes--;
        }

        if (verifyExecuteOnAllWorkersWithRange) {
            verify(remoteClient, atLeast((executeOnAllWorkersTimes - 1) * numberOfTests))
                    .sendToAllWorkers(any(SimulatorOperation.class));
            verify(remoteClient, atMost(executeOnAllWorkersTimes * numberOfTests)).sendToAllWorkers(any(SimulatorOperation.class));
        } else {
            verify(remoteClient, times(executeOnAllWorkersTimes * numberOfTests)).sendToAllWorkers(any(SimulatorOperation.class));
        }
        verify(remoteClient, times(executeOnFirstWorkerTimes * numberOfTests)).sendToFirstWorker(any(SimulatorOperation.class));
        verify(remoteClient, times(1)).terminateWorkers(true);
    }

    private class TestPhaseCompleter extends Thread {

        private final TestPhaseListenerContainer testPhaseListenerContainer;
        private final FailureContainer failureContainer;

        public TestPhaseCompleter(Coordinator coordinator) {
            super("TestPhaseCompleter");

            this.testPhaseListenerContainer = coordinator.getTestPhaseListenerContainer();
            this.failureContainer = coordinator.getFailureContainer();

            setDaemon(true);
            start();
        }

        @Override
        public void run() {
            for (TestPhase testPhase : TestPhase.values()) {
                sleepMillis(100);
                for (TestCase testCase : testSuite.getTestCaseList()) {
                    testPhaseListenerContainer.updatePhaseCompletion(testCase.getId(), testPhase);
                }
            }

            sleepMillis(100);
            SimulatorAddress workerAddress = new SimulatorAddress(AddressLevel.WORKER, 1, 1, 0);
            FailureOperation operation = new FailureOperation("worker finished", WORKER_FINISHED, workerAddress, "127.0.0.1",
                    "127.0.0.1:5701", "workerId", "testId", testSuite, "stacktrace");
            failureContainer.addFailureOperation(operation);
        }
    }
}
