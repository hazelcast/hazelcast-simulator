package com.hazelcast.simulator.coordinator;

import com.hazelcast.simulator.common.AgentsFile;
import com.hazelcast.simulator.common.SimulatorProperties;
import com.hazelcast.simulator.coordinator.remoting.AgentsClient;
import com.hazelcast.simulator.coordinator.remoting.NewProtocolAgentsClient;
import com.hazelcast.simulator.probes.probes.Result;
import com.hazelcast.simulator.probes.probes.impl.ThroughputResult;
import com.hazelcast.simulator.test.Failure;
import com.hazelcast.simulator.test.TestCase;
import com.hazelcast.simulator.test.TestPhase;
import com.hazelcast.simulator.test.TestSuite;
import com.hazelcast.simulator.worker.commands.Command;
import com.hazelcast.simulator.worker.commands.GetBenchmarkResultsCommand;
import com.hazelcast.simulator.worker.commands.StopCommand;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.reflect.Whitebox;

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
    private final CoordinatorParameters parameters = mock(CoordinatorParameters.class);

    @Mock
    private final AgentsClient agentsClient = mock(AgentsClient.class);

    @Mock
    private final NewProtocolAgentsClient newProtocolAgentsClient = mock(NewProtocolAgentsClient.class);

    @Mock
    private final FailureMonitor failureMonitor = mock(FailureMonitor.class);

    @Mock
    private final PerformanceMonitor performanceMonitor = mock(PerformanceMonitor.class);

    private Coordinator coordinator;

    private boolean parallel = false;
    private boolean verifyEnabled = true;
    private boolean monitorPerformance = false;

    @BeforeClass
    public static void setUp() throws Exception {
        userDir = System.getProperty("user.dir");
        System.setProperty("user.dir", "./dist/src/main/dist");
    }

    @AfterClass
    public static void tearDown() {
        System.setProperty("user.dir", userDir);
    }

    @After
    public void cleanUp() {
        deleteQuiet(new File("probes-" + testSuite.id + "_CoordinatorTest1.xml"));
        deleteQuiet(new File("probes-" + testSuite.id + "_CoordinatorTest2.xml"));
    }

    @Test
    public void runTestSuiteParallel_waitForTestCase_and_duration() throws Exception {
        testSuite.waitForTestCase = true;
        testSuite.durationSeconds = 3;
        parallel = true;
        initMocks();

        coordinator.runTestSuite();

        verifyAgentsClient(coordinator);
    }

    @Test
    public void runTestSuiteParallel_waitForTestCase_noVerify() throws Exception {
        testSuite.waitForTestCase = true;
        testSuite.durationSeconds = 0;
        parallel = true;
        verifyEnabled = false;
        initMocks();

        coordinator.runTestSuite();

        verifyAgentsClient(coordinator);
    }

    @Test
    public void runTestSuiteParallel_performanceMonitorEnabled() throws Exception {
        testSuite.durationSeconds = 4;
        parallel = true;
        monitorPerformance = true;
        initMocks();

        coordinator.runTestSuite();

        verifyAgentsClient(coordinator);
    }

    @Test
    public void runTestSuiteSequential_hasCriticalFailures() throws Exception {
        when(failureMonitor.hasCriticalFailure(anySetOf(Failure.Type.class))).thenReturn(true);

        testSuite.durationSeconds = 4;
        parallel = false;
        initMocks();

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
                resultMap.put("CoordinatorTest1", new ThroughputResult(1000, 23.42f));
                resultMap.put("CoordinatorTest2", new ThroughputResult(2000, 42.23f));

                List<Map<String, Result>> resultList = new ArrayList<Map<String, Result>>();
                resultList.add(resultMap);

                List<List<Map<String, Result>>> result = new ArrayList<List<Map<String, Result>>>();
                result.add(resultList);

                return result;
            }
        };
        when(agentsClient.executeOnAllWorkers(isA(GetBenchmarkResultsCommand.class))).thenAnswer(probeResultsAnswer);

        testSuite.durationSeconds = 1;
        parallel = false;
        initMocks();

        coordinator.runTestSuite();

        verifyAgentsClient(coordinator);
    }

    @Test
    public void runTestSuite_getProbeResultsTimeoutException() throws Exception {
        when(agentsClient.executeOnAllWorkers(isA(GetBenchmarkResultsCommand.class))).thenThrow(new TimeoutException());

        testSuite.durationSeconds = 1;
        parallel = true;
        initMocks();

        coordinator.runTestSuite();
    }

    @Test
    public void runTestSuite_stopThreadTimeoutException() throws Exception {
        when(agentsClient.executeOnAllWorkers(isA(StopCommand.class))).thenThrow(new TimeoutException());

        testSuite.durationSeconds = 1;
        parallel = true;
        initMocks();

        coordinator.runTestSuite();
    }

    @Test
    public void runTestSuite_withException() throws Exception {
        when(agentsClient.executeOnAllWorkers(any(Command.class))).thenThrow(new RuntimeException("expected"));

        testSuite.durationSeconds = 1;
        initMocks();

        coordinator.runTestSuite();
    }

    private void initMocks() {
        // CoordinatorParameters
        SimulatorProperties simulatorProperties = new SimulatorProperties();

        when(parameters.getSimulatorProperties()).thenReturn(simulatorProperties);
        when(parameters.getAgentsFile()).thenReturn(new File(AgentsFile.NAME));
        when(parameters.isParallel()).thenReturn(parallel);
        when(parameters.isVerifyEnabled()).thenReturn(verifyEnabled);
        when(parameters.isMonitorPerformance()).thenReturn(monitorPerformance);

        // TestSuite
        TestCase testCase1 = new TestCase("CoordinatorTest1");
        TestCase testCase2 = new TestCase("CoordinatorTest2");

        testSuite.addTest(testCase1);
        testSuite.addTest(testCase2);

        // FailureMonitor
        when(failureMonitor.getFailureCount()).thenReturn(0);

        // PerformanceMonitor
        when(performanceMonitor.getPerformanceNumbers()).thenReturn(" (PerformanceMonitor is mocked)");

        // Coordinator
        coordinator = new Coordinator(parameters, testSuite);
        coordinator.cooldownSeconds = 0;
        coordinator.testCaseRunnerSleepPeriod = 3;

        Whitebox.setInternalState(coordinator, "agentsClient", agentsClient);
        Whitebox.setInternalState(coordinator, "newProtocolAgentsClient", newProtocolAgentsClient);
        Whitebox.setInternalState(coordinator, "failureMonitor", failureMonitor);
        Whitebox.setInternalState(coordinator, "performanceMonitor", performanceMonitor);
    }

    private void verifyAgentsClient(Coordinator coordinator) throws Exception {
        boolean verifyExecuteOnAllWorkersWithRange = false;
        int numberOfTests = testSuite.size();
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
        if (testSuite.durationSeconds == 0) {
            // no StopCommand is sent
            executeOnAllWorkersTimes--;
        } else if (testSuite.waitForTestCase) {
            // has duration and waitForTestCase
            waitForPhaseCompletionTimes++;
            verifyExecuteOnAllWorkersWithRange = true;
        }
        if (!coordinator.getParameters().isVerifyEnabled()) {
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
        if (verifyExecuteOnAllWorkersWithRange) {
            verify(agentsClient, atLeast(waitForPhaseCompletionTimes - 1))
                    .waitForPhaseCompletion(anyString(), eq("CoordinatorTest1"), any(TestPhase.class));
            verify(agentsClient, atMost(waitForPhaseCompletionTimes))
                    .waitForPhaseCompletion(anyString(), eq("CoordinatorTest1"), any(TestPhase.class));
            verify(agentsClient, atLeast(waitForPhaseCompletionTimes - 1))
                    .waitForPhaseCompletion(anyString(), eq("CoordinatorTest2"), any(TestPhase.class));
            verify(agentsClient, atMost(waitForPhaseCompletionTimes))
                    .waitForPhaseCompletion(anyString(), eq("CoordinatorTest2"), any(TestPhase.class));
        }
        verify(agentsClient, times(1)).terminateWorkers();
    }
}
