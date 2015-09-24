package com.hazelcast.simulator.coordinator;

import com.hazelcast.simulator.common.AgentsFile;
import com.hazelcast.simulator.common.SimulatorProperties;
import com.hazelcast.simulator.coordinator.remoting.AgentsClient;
import com.hazelcast.simulator.coordinator.remoting.RemoteClient;
import com.hazelcast.simulator.probes.probes.Result;
import com.hazelcast.simulator.probes.probes.impl.ThroughputResult;
import com.hazelcast.simulator.protocol.operation.SimulatorOperation;
import com.hazelcast.simulator.test.Failure;
import com.hazelcast.simulator.test.TestCase;
import com.hazelcast.simulator.test.TestPhase;
import com.hazelcast.simulator.test.TestSuite;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
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

import static com.hazelcast.simulator.utils.FileUtils.deleteQuiet;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anySetOf;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.doThrow;
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
    private final RemoteClient remoteClient = mock(RemoteClient.class);

    @Mock
    private final FailureMonitor failureMonitor = mock(FailureMonitor.class);

    @Mock
    private final PerformanceStateContainer performanceStateContainer = mock(PerformanceStateContainer.class);

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
        deleteQuiet(new File("probes-" + testSuite.getId() + "_CoordinatorTest1.xml"));
        deleteQuiet(new File("probes-" + testSuite.getId() + "_CoordinatorTest2.xml"));
    }

    @Test
    public void runTestSuiteParallel_waitForTestCase_and_duration() throws Exception {
        testSuite.setWaitForTestCase(true);
        testSuite.setDurationSeconds(3);
        parallel = true;
        initMocks();

        coordinator.runTestSuite();

        verifyRemoteClient(coordinator);
    }

    @Test
    public void runTestSuiteParallel_waitForTestCase_noVerify() throws Exception {
        testSuite.setWaitForTestCase(true);
        testSuite.setDurationSeconds(0);
        parallel = true;
        verifyEnabled = false;
        initMocks();

        coordinator.runTestSuite();

        verifyRemoteClient(coordinator);
    }

    @Test
    public void runTestSuiteParallel_performanceMonitorEnabled() throws Exception {
        testSuite.setDurationSeconds(4);
        parallel = true;
        monitorPerformance = true;
        initMocks();

        coordinator.runTestSuite();

        verifyRemoteClient(coordinator);
    }

    @Test
    public void runTestSuiteSequential_hasCriticalFailures() throws Exception {
        when(failureMonitor.hasCriticalFailure(anySetOf(Failure.Type.class))).thenReturn(true);

        testSuite.setDurationSeconds(4);
        parallel = false;
        initMocks();

        coordinator.runTestSuite();

        verifyRemoteClient(coordinator);
    }

    // FIXME
    @Ignore
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
        //when(agentsClient.executeOnAllWorkers(isA(GetBenchmarkResultsCommand.class))).thenAnswer(probeResultsAnswer);

        testSuite.setDurationSeconds(1);
        parallel = false;
        initMocks();

        coordinator.runTestSuite();

        verifyRemoteClient(coordinator);
    }

    // FIXME
    @Ignore
    @Test
    public void runTestSuite_getProbeResultsTimeoutException() throws Exception {
        //when(agentsClient.executeOnAllWorkers(isA(GetBenchmarkResultsCommand.class))).thenThrow(new TimeoutException());

        testSuite.setDurationSeconds(1);
        parallel = true;
        initMocks();

        coordinator.runTestSuite();
    }

    @Test
    public void runTestSuite_withException() throws Exception {
        doThrow(new RuntimeException("expected")).when(remoteClient).sendToAllWorkers(any(SimulatorOperation.class));

        testSuite.setDurationSeconds(1);
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

        // PerformanceStateContainer
        when(performanceStateContainer.getPerformanceNumbers(anyString())).thenReturn(" (PerformanceStateContainer is mocked)");

        // Coordinator
        coordinator = new Coordinator(parameters, testSuite, 0, 3);

        Whitebox.setInternalState(coordinator, "agentsClient", agentsClient);
        Whitebox.setInternalState(coordinator, "remoteClient", remoteClient);
        Whitebox.setInternalState(coordinator, "failureMonitor", failureMonitor);
        Whitebox.setInternalState(coordinator, "performanceStateContainer", performanceStateContainer);
    }

    private void verifyRemoteClient(Coordinator coordinator) throws Exception {
        boolean verifyExecuteOnAllWorkersWithRange = false;
        int numberOfTests = testSuite.size();
        int phaseNumber = TestPhase.values().length;
        int executeOnFirstWorkerTimes = 0;
        int executeOnAllWorkersTimes = 2; // InitCommand and StopCommand
        for (TestPhase testPhase : TestPhase.values()) {
            if (testPhase.desc().startsWith("global")) {
                executeOnFirstWorkerTimes++;
            } else {
                executeOnAllWorkersTimes++;
            }
        }
        int waitForPhaseCompletionTimes = phaseNumber;
        if (testSuite.getDurationSeconds() == 0) {
            // no StopCommand is sent
            executeOnAllWorkersTimes--;
        } else if (testSuite.isWaitForTestCase()) {
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
            verify(remoteClient, atLeast((executeOnAllWorkersTimes - 1) * numberOfTests))
                    .sendToAllWorkers(any(SimulatorOperation.class));
            verify(remoteClient, atMost(executeOnAllWorkersTimes * numberOfTests)).sendToAllWorkers(any(SimulatorOperation.class));
        } else {
            verify(remoteClient, times(executeOnAllWorkersTimes * numberOfTests)).sendToAllWorkers(any(SimulatorOperation.class));
        }
        verify(remoteClient, times(executeOnFirstWorkerTimes * numberOfTests)).sendToFirstWorker(any(SimulatorOperation.class));
        if (verifyExecuteOnAllWorkersWithRange) {
            verify(remoteClient, atLeast(waitForPhaseCompletionTimes - 1))
                    .waitForPhaseCompletion(anyString(), eq("CoordinatorTest1"), any(TestPhase.class));
            verify(remoteClient, atMost(waitForPhaseCompletionTimes))
                    .waitForPhaseCompletion(anyString(), eq("CoordinatorTest1"), any(TestPhase.class));
            verify(remoteClient, atLeast(waitForPhaseCompletionTimes - 1))
                    .waitForPhaseCompletion(anyString(), eq("CoordinatorTest2"), any(TestPhase.class));
            verify(remoteClient, atMost(waitForPhaseCompletionTimes))
                    .waitForPhaseCompletion(anyString(), eq("CoordinatorTest2"), any(TestPhase.class));
        }
        verify(remoteClient, times(1)).terminateWorkers();
    }
}
