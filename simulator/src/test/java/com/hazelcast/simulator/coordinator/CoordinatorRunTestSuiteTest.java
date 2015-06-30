package com.hazelcast.simulator.coordinator;

import com.hazelcast.simulator.agent.workerjvm.WorkerJvmSettings;
import com.hazelcast.simulator.coordinator.remoting.AgentsClient;
import com.hazelcast.simulator.probes.probes.Result;
import com.hazelcast.simulator.probes.probes.impl.OperationsPerSecResult;
import com.hazelcast.simulator.test.Failure;
import com.hazelcast.simulator.test.TestCase;
import com.hazelcast.simulator.test.TestPhase;
import com.hazelcast.simulator.test.TestSuite;
import com.hazelcast.simulator.worker.commands.Command;
import com.hazelcast.simulator.worker.commands.GetBenchmarkResultsCommand;
import com.hazelcast.simulator.worker.commands.StopCommand;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import static com.hazelcast.simulator.utils.FileUtils.deleteQuiet;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anySetOf;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CoordinatorRunTestSuiteTest {

    private static String userDir;

    private final TestSuite testSuite = new TestSuite();

    @Mock
    private final WorkerJvmSettings workerJvmSettings = new WorkerJvmSettings();

    @Mock
    private final AgentsClient agentsClient = mock(AgentsClient.class);

    @Mock
    private final FailureMonitor failureMonitor = mock(FailureMonitor.class);

    @Mock
    private final PerformanceMonitor performanceMonitor = mock(PerformanceMonitor.class);

    @InjectMocks
    private Coordinator coordinator;

    @BeforeClass
    public static void setUp() throws Exception {
        userDir = System.getProperty("user.dir");
        System.setProperty("user.dir", "./dist/src/main/dist");
    }

    @AfterClass
    public static void tearDown() {
        System.setProperty("user.dir", userDir);
    }

    @Before
    public void initMocks() {
        MockitoAnnotations.initMocks(this);

        List<String> privateAddressList = new ArrayList<String>(1);
        privateAddressList.add("127.0.0.1");

        TestCase testCase1 = new TestCase("CoordinatorTest1");
        TestCase testCase2 = new TestCase("CoordinatorTest2");

        testSuite.addTest(testCase1);
        testSuite.addTest(testCase2);

        coordinator.testSuite = testSuite;
        coordinator.cooldownSeconds = 0;
        coordinator.testCaseRunnerSleepPeriod = 3;
        coordinator.lastTestPhaseToSync = TestPhase.SETUP;

        when(agentsClient.getPublicAddresses()).thenReturn(privateAddressList);
        when(agentsClient.getAgentCount()).thenReturn(1);

        when(failureMonitor.getFailureCount()).thenReturn(0);

        when(performanceMonitor.getPerformanceNumbers()).thenReturn(" (PerformanceMonitor is mocked)");
    }

    @After
    public void cleanUp() {
        deleteQuiet(new File("probes-" + testSuite.id + "_CoordinatorTest1.xml"));
        deleteQuiet(new File("probes-" + testSuite.id + "_CoordinatorTest2.xml"));
    }

    @Test
    public void runTestSuiteParallel_waitForTestCase_and_duration() throws Exception {
        coordinator.testSuite.waitForTestCase = true;
        coordinator.testSuite.durationSeconds = 3;
        coordinator.parallel = true;

        coordinator.runTestSuite();

        verifyAgentsClient(coordinator);
    }

    @Test
    public void runTestSuiteParallel_waitForTestCase_noVerify() throws Exception {
        coordinator.testSuite.waitForTestCase = true;
        coordinator.testSuite.durationSeconds = 0;
        coordinator.parallel = true;
        coordinator.verifyEnabled = false;

        coordinator.runTestSuite();

        verifyAgentsClient(coordinator);
    }

    @Test
    public void runTestSuiteParallel_performanceMonitorEnabled() throws Exception {
        coordinator.testSuite.durationSeconds = 4;
        coordinator.parallel = true;
        coordinator.monitorPerformance = true;

        coordinator.runTestSuite();

        verifyAgentsClient(coordinator);
    }

    @Test
    public void runTestSuiteSequential_hasCriticalFailures() throws Exception {
        when(failureMonitor.hasCriticalFailure(anySetOf(Failure.Type.class))).thenReturn(true);

        coordinator.testSuite.durationSeconds = 4;
        coordinator.parallel = false;

        coordinator.runTestSuite();

        verifyAgentsClient(coordinator);
    }

    @Test
    public void runTestSuiteSequential_probeResults() throws Exception {
        Answer<List<List<Map<String, Result>>>> probeResultsAnswer = new Answer<List<List<Map<String, Result>>>>() {
            @Override
            @SuppressWarnings("unchecked")
            public List<List<Map<String, Result>>> answer(InvocationOnMock invocation) throws Throwable {
                Map<String, Result> resultMap = new HashMap<String, Result>();
                resultMap.put("CoordinatorTest1", new OperationsPerSecResult(1000, 23.42f));
                resultMap.put("CoordinatorTest2", new OperationsPerSecResult(2000, 42.23f));

                List<Map<String, Result>> resultList = new ArrayList<Map<String, Result>>();
                resultList.add(resultMap);

                List<List<Map<String, Result>>> result = new ArrayList<List<Map<String, Result>>>();
                result.add(resultList);

                return result;
            }
        };
        when(agentsClient.executeOnAllWorkers(isA(GetBenchmarkResultsCommand.class))).thenAnswer(probeResultsAnswer);

        coordinator.testSuite.durationSeconds = 1;
        coordinator.parallel = false;

        coordinator.runTestSuite();

        verifyAgentsClient(coordinator);
    }

    @Test
    public void runTestSuite_getProbeResultsTimeoutException() throws Exception {
        when(agentsClient.executeOnAllWorkers(isA(GetBenchmarkResultsCommand.class))).thenThrow(new TimeoutException());

        coordinator.testSuite.durationSeconds = 1;
        coordinator.parallel = true;

        coordinator.runTestSuite();
    }

    @Test
    public void runTestSuite_stopThreadTimeoutException() throws Exception {
        when(agentsClient.executeOnAllWorkers(isA(StopCommand.class))).thenThrow(new TimeoutException());

        coordinator.testSuite.durationSeconds = 1;
        coordinator.parallel = true;

        coordinator.runTestSuite();
    }

    @Test
    public void runTestSuite_withException() throws Exception {
        when(agentsClient.executeOnAllWorkers(any(Command.class))).thenThrow(new RuntimeException("expected"));

        coordinator.runTestSuite();
    }

    private void verifyAgentsClient(Coordinator coordinator) throws Exception {
        boolean verifyExecuteOnAllWorkersWithRange = false;
        int numberOfTests = coordinator.testSuite.size();
        int phaseNumber = TestPhase.values().length;
        int executeOnFirstWorkerTimes = 0;
        int executeOnAllWorkersTimes = 3; // InitCommand, StopCommand and GetBenchmarkResultsCommand
        for (TestPhase testPhase : TestPhase.values()) {
            if (testPhase.desc().startsWith("global")) {
                executeOnFirstWorkerTimes++;
            } else {
                executeOnAllWorkersTimes++;
            }
        }
        int waitForPhaseCompletionTimes = phaseNumber;
        if (coordinator.testSuite.durationSeconds == 0) {
            // no StopCommand is sent
            executeOnAllWorkersTimes--;
        } else if (coordinator.testSuite.waitForTestCase) {
            // has duration and waitForTestCase
            waitForPhaseCompletionTimes++;
            verifyExecuteOnAllWorkersWithRange = true;
        }
        if (!coordinator.verifyEnabled) {
            // no GenericCommand for global and local verify phase are sent
            executeOnFirstWorkerTimes--;
            executeOnAllWorkersTimes--;
            waitForPhaseCompletionTimes -= 2;
        }

        if (verifyExecuteOnAllWorkersWithRange) {
            verify(agentsClient, atLeast((executeOnAllWorkersTimes - 1) * numberOfTests)).executeOnAllWorkers(any(Command.class));
            verify(agentsClient, atMost(executeOnAllWorkersTimes * numberOfTests)).executeOnAllWorkers(any(Command.class));
        } else {
            verify(agentsClient, times(executeOnAllWorkersTimes * numberOfTests)).executeOnAllWorkers(any(Command.class));
        }
        verify(agentsClient, times(executeOnFirstWorkerTimes * numberOfTests)).executeOnFirstWorker(any(Command.class));
        verify(agentsClient, times(waitForPhaseCompletionTimes))
                .waitForPhaseCompletion(anyString(), eq("CoordinatorTest1"), any(TestPhase.class));
        verify(agentsClient, times(waitForPhaseCompletionTimes))
                .waitForPhaseCompletion(anyString(), eq("CoordinatorTest2"), any(TestPhase.class));
        verify(agentsClient, times(1)).terminateWorkers();
    }
}
