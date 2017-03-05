package com.hazelcast.simulator.coordinator.tasks;

import com.hazelcast.simulator.agent.Agent;
import com.hazelcast.simulator.common.SimulatorProperties;
import com.hazelcast.simulator.coordinator.CoordinatorParameters;
import com.hazelcast.simulator.coordinator.FailureCollector;
import com.hazelcast.simulator.coordinator.PerformanceStatsCollector;
import com.hazelcast.simulator.coordinator.WorkerParameters;
import com.hazelcast.simulator.coordinator.operations.FailureOperation;
import com.hazelcast.simulator.coordinator.registry.ComponentRegistry;
import com.hazelcast.simulator.protocol.CoordinatorClient;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.simulator.TestEnvironmentUtils.internalDistPath;
import static com.hazelcast.simulator.TestEnvironmentUtils.localResourceDirectory;
import static com.hazelcast.simulator.TestEnvironmentUtils.setupFakeEnvironment;
import static com.hazelcast.simulator.TestEnvironmentUtils.tearDownFakeEnvironment;
import static com.hazelcast.simulator.utils.FileUtils.deleteQuiet;
import static com.hazelcast.simulator.utils.FileUtils.fileAsText;
import static com.hazelcast.simulator.utils.HazelcastUtils.initMemberHzConfig;
import static com.hazelcast.simulator.utils.SimulatorUtils.localIp;
import static com.hazelcast.simulator.utils.TestUtils.createTmpDirectory;

public class RunTestSuiteTaskTest {
//
//    private static final AtomicLong ID_GENERATOR = new AtomicLong();
//    private static SimulatorAddress agentAddress;
//    private CountDownLatch finishWorkerLatch = new CountDownLatch(1);
//
//    private File outputDirectory;
//    private FailureOperation criticalFailureOperation;
//
//    private static SimulatorProperties simulatorProperties;
//    private static ComponentRegistry componentRegistry;
//    private FailureCollector failureCollector;
//    private static CoordinatorClient client;
//
//    private boolean parallel = false;
//    private boolean verifyEnabled = true;
//    private int monitorPerformanceMonitorIntervalSeconds = 0;
//    private static Agent agent;
//    private String sessionId;
//    private PerformanceStatsCollector performanceStatsCollector;
//    private CoordinatorParameters coordinatorParameters;
//
//    @BeforeClass
//    public static void prepareEnvironment() throws Exception {
//        setupFakeEnvironment();
//
//        agent = new Agent(1, localIp(), SimulatorProperties.DEFAULT_AGENT_PORT, 10, 60);
//        agent.start();
//        componentRegistry = new ComponentRegistry();
//        componentRegistry.addAgent(localIp(), localIp());
//
//        agentAddress = SimulatorAddress.fromString("C_A1");
//
//        client = new CoordinatorClient().start();
//
//        client.connectToAgentBroker(agentAddress, localIp());
//        simulatorProperties = new SimulatorProperties();
//    }
//
//    @AfterClass
//    public static void afterClass() {
//        agent.close();
//    }
//
//    @Before
//    public void before() throws ExecutionException, InterruptedException {
//        sessionId = "foo";
//
//        performanceStatsCollector = new PerformanceStatsCollector();
//
//        agent.getProcessManager().setSessionId(sessionId);
//
//        outputDirectory = createTmpDirectory();
//
//        coordinatorParameters = new CoordinatorParameters()
//                .setSessionId(sessionId)
//                .setSimulatorProperties(simulatorProperties);
//
//
//        String hzConfig = fileAsText(new File(localResourceDirectory(), "hazelcast.xml"));
//        initMemberHzConfig(hzConfig, componentRegistry, null, simulatorProperties.asMap(), false);
//
//        File scriptFile = new File(internalDistPath() + "/conf/worker.sh");
//        File logFile = new File(internalDistPath() + "/conf/agent-log4j.xml");
//
//        WorkerParameters workerParameters = new WorkerParameters()
//                .setVersionSpec(simulatorProperties.getVersionSpec())
//                .addEnvironment(simulatorProperties.asMap())
//                .addEnvironment("HAZELCAST_CONFIG", hzConfig)
//                .addEnvironment("LOG4j_CONFIG", fileAsText(logFile))
//                .addEnvironment("AUTOCREATE_HAZELCAST_INSTANCE", "true")
//                .addEnvironment("JVM_OPTIONS", "")
//                .addEnvironment("WORKER_PERFORMANCE_MONITOR_INTERVAL_SECONDS", "10")
//                .addEnvironment("VENDOR", "hazelcast")
//                .addEnvironment("WORKER_TYPE", "member")
//                .setWorkerStartupTimeout(simulatorProperties.getWorkerStartupTimeoutSeconds())
//                .setWorkerScript(fileAsText(scriptFile));
//
//        //WorkerProcessSettings settings = new WorkerProcessSettings(1, WorkerType.MEMBER,"maven=3.8",)
//
//        //client.submit(agentAddress, new CreateWorkerOperation(asList(workerParameters), 0)).get();
//    }
//
//    @After
//    public void cleanUp() {
//        tearDownFakeEnvironment();
//        deleteQuiet(outputDirectory);
//    }
//
//    @Test
//    public void runParallel_waitForTestCase_and_duration() {
//        TestSuite suite = new TestSuite()
//                .addTest(new TestCase("foo")
//                        .setProperty("class", SuccessTest.class))
//                .setParallel(true)
//                .setDurationSeconds(3);
//
//        parallel = true;
//
//        RunTestSuiteTask task = new RunTestSuiteTask(
//                suite, coordinatorParameters, componentRegistry, failureCollector, client, performanceStatsCollector);
//        task.run();
//    }

//    @Test
//    public void runParallel_waitForTestCase_noVerify() {
//        testSuite.setDurationSeconds(0);
//        parallel = true;
//        verifyEnabled = false;
//
//        RunTestSuiteTask task = createRunTestSuiteTask();
//        task.run();
//
//        verifyRemoteClient();
//    }
//
//    @Test
//    public void runParallel_performanceMonitorEnabled() {
//        testSuite.setDurationSeconds(4);
//        parallel = true;
//        monitorPerformanceMonitorIntervalSeconds = 10;
//
//        RunTestSuiteTask task = createRunTestSuiteTask();
//        task.run();
//
//        verifyRemoteClient();
//    }
//
//    @Test
//    public void runParallel_withTargetCount() {
//        testSuite.setDurationSeconds(0);
//        parallel = true;
//        verifyEnabled = false;
//
//        RunTestSuiteTask task = createRunTestSuiteTask(1);
//        task.run();
//
//        verifyRemoteClient();
//    }
//
//    @Test
//    public void runParallel_withWarmup() {
//        testSuite.setDurationSeconds(1);
//        parallel = true;
//        verifyEnabled = false;
//
//        RunTestSuiteTask task = createRunTestSuiteTask();
//        task.run();
//
//        verifyRemoteClient();
//    }
//
//    @Test
//    public void runParallel_withWarmup_waitForTestCase() {
//        testSuite.setDurationSeconds(0);
//        parallel = true;
//        verifyEnabled = false;
//
//        RunTestSuiteTask task = createRunTestSuiteTask();
//        task.run();
//
//        verifyRemoteClient();
//    }
//
//    @Test
//    public void runSequential_withSingleTest() {
//        TestCase testCase = new TestCase("CoordinatorTest" + ID_GENERATOR.incrementAndGet());
//
//        testSuite = new TestSuite();
//        testSuite.addTest(testCase);
//        testSuite.setDurationSeconds(1);
//
//        RunTestSuiteTask task = createRunTestSuiteTask();
//        task.run();
//
//        verifyRemoteClient();
//    }
//
//    @Test
//    public void runParallel_withSingleTest() {
//        TestCase testCase = new TestCase("CoordinatorTest" + ID_GENERATOR.incrementAndGet());
//
//        testSuite = new TestSuite();
//        testSuite.addTest(testCase);
//        testSuite.setDurationSeconds(1);
//
//        parallel = true;
//
//        RunTestSuiteTask task = createRunTestSuiteTask();
//        task.run();
//
//        verifyRemoteClient();
//    }
//
//    @Test
//    public void runSequential_hasCriticalFailures() {
//        testSuite.setDurationSeconds(4);
//        parallel = false;
//
//        RunTestSuiteTask task = createRunTestSuiteTask();
//        failureCollector.notify(criticalFailureOperation);
//        task.run();
//    }
//
//    @Test
//    public void runParallel_hasCriticalFailures() {
//        testSuite.setDurationSeconds(4);
//        testSuite.setFailFast(false);
//        parallel = true;
//
//        RunTestSuiteTask task = createRunTestSuiteTask();
//        failureCollector.notify(criticalFailureOperation);
//        task.run();
//    }
//
//    @Test
//    public void runSequential_hasCriticalFailures_withFailFast() {
//        testSuite.setDurationSeconds(1);
//        testSuite.setFailFast(true);
//
//        RunTestSuiteTask task = createRunTestSuiteTask();
//        failureCollector.notify(criticalFailureOperation);
//        task.run();
//    }
//
//    @Test
//    public void runParallel_hasCriticalFailures_withFailFast() {
//        testSuite.setDurationSeconds(1);
//        testSuite.setFailFast(true);
//        parallel = true;
//
//        RunTestSuiteTask task = createRunTestSuiteTask();
//        failureCollector.notify(criticalFailureOperation);
//        task.run();
//    }
//
//    @Test(expected = IllegalStateException.class)
//    public void runSequential_withException() {
//        // doThrow(new IllegalStateException("expected")).when(client).invokeOnAllWorkers(any(SimulatorOperation.class));
//        testSuite.setDurationSeconds(1);
//        parallel = false;
//
//        RunTestSuiteTask task = createRunTestSuiteTask();
//        task.run();
//    }
//
//    @Test(expected = IllegalStateException.class)
//    public void runParallel_withException() {
//        // doThrow(new IllegalStateException("expected")).when(client).invokeOnAllWorkers(any(SimulatorOperation.class));
//        testSuite.setDurationSeconds(1);
//        parallel = true;
//
//        RunTestSuiteTask task = createRunTestSuiteTask();
//        task.run();
//    }
//
//    @Test
//    public void runSequential_withWorkerNotShuttingDown() {
//        simulatorProperties.set("WAIT_FOR_WORKER_SHUTDOWN_TIMEOUT_SECONDS", "1");
//        testSuite.setDurationSeconds(1);
//        finishWorkerLatch = null;
//
//        RunTestSuiteTask task = createRunTestSuiteTask();
//        task.run();
//
//        verifyRemoteClient();
//    }
//
//    private RunTestSuiteTask createRunTestSuiteTask() {
//        return createRunTestSuiteTask(0);
//    }

//    private RunTestSuiteTask createRunTestSuiteTask(int targetCount) {
//        WorkerProcessSettings workerProcessSettings = mock(WorkerProcessSettings.class);
//        when(workerProcessSettings.getWorkerIndex()).thenReturn(1);
//        when(workerProcessSettings.getWorkerType()).thenReturn(WorkerType.MEMBER);
//
//        componentRegistry = new ComponentRegistry();
//        componentRegistry.addAgent("127.0.0.1", "127.0.0.1");
//        componentRegistry.addWorkers(componentRegistry.getFirstAgent().getAddress(), singletonList(workerProcessSettings));
//
//        failureCollector = new FailureCollector(outputDirectory, componentRegistry);
//        PerformanceStatsCollector performanceStatsCollector = new PerformanceStatsCollector();
//
//        CoordinatorParameters coordinatorParameters = new CoordinatorParameters()
//                .setPerformanceMonitorIntervalSeconds(monitorPerformanceMonitorIntervalSeconds)
//                .setSimulatorProperties(simulatorProperties);
//
//        WorkerQuery query = new WorkerQuery().setTargetType(TargetType.ALL);
//        if (targetCount > 0) {
//            query.setMaxCount(targetCount);
//        }
//        testSuite.setVerifyEnabled(verifyEnabled)
//                .setParallel(parallel)
//                .setWorkerQuery(query);
//
//        RunTestSuiteTask task = new RunTestSuiteTask(testSuite, coordinatorParameters, componentRegistry, failureCollector,
//                client, performanceStatsCollector);
//
//        return task;
//    }

    private void verifyRemoteClient() {
//        int testCount = testSuite.size();
//        List<TestPhase> expectedTestPhases = getExpectedTestPhases();
//
//        in the remainingPhaseCount the remaining number of calls per phase.Eventually everything should be 0.
//        Map<TestPhase, AtomicInteger> remainingPhaseCount = new HashMap<TestPhase, AtomicInteger>();
//        for (TestPhase testPhase : expectedTestPhases) {
//            remainingPhaseCount.put(testPhase, new AtomicInteger(testCount));
//        }
//
//        we check if the create calls have been made
//        verify(client, times(testCount)).invokeOnAllWorkers(any(CreateTestOperation.class));
//
//        now we suck up all 'invokeOnTestOnAllWorkers'
//        ArgumentCaptor<SimulatorOperation> allTestOperations = ArgumentCaptor.forClass(SimulatorOperation.class);
//        verify(client, atLeast(0)).invokeOnTestOnAllWorkers(any(SimulatorAddress.class), allTestOperations.capture());
//
//        now we suck up all 'invokeOnTestOnFirstWorker'
//        ArgumentCaptor<SimulatorOperation> firstTestOperations = ArgumentCaptor.forClass(SimulatorOperation.class);
//        verify(client, atLeast(0)).invokeOnTestOnFirstWorker(any(SimulatorAddress.class), firstTestOperations.capture());
//
//        int actualStopTestCount = 0;
//
//
//        for (SimulatorOperation operation : allTestOperations.getAllValues()) {
//            if (operation instanceof StartPhaseOperation) {
//                remainingPhaseCount.get(((StartPhaseOperation) operation).getTestPhase()).decrementAndGet();
//            } else if (operation instanceof StartRunOperation) {
//                TestPhase phase = RUN;
//                remainingPhaseCount.get(phase).decrementAndGet();
//            } else if (operation instanceof StopRunOperation) {
//                actualStopTestCount++;
//            } else {
//                fail("Unrecognized operation: " + operation);
//            }
//        }
//
//        for (SimulatorOperation operation : firstTestOperations.getAllValues()) {
//            if (operation instanceof StartPhaseOperation) {
//                remainingPhaseCount.get(((StartPhaseOperation) operation).getTestPhase()).decrementAndGet();
//            } else {
//                fail("Unrecognized operation: " + operation);
//            }
//        }
//
//        int expectedStopCount = testCount;
//        assertEquals("actualStopTestCount incorrect", expectedStopCount, actualStopTestCount);
//
//        for (Map.Entry<TestPhase, AtomicInteger> entry : remainingPhaseCount.entrySet()) {
//            TestPhase phase = entry.getKey();
//            int value = entry.getAnswer().get();
//            assertEquals("Number of remaining occurrences for phase: " + phase + " incorrect", 0, value);
//        }
    }

//    private List<TestPhase> getExpectedTestPhases() {
//        //per default we expected all test phases to be called
//        List<TestPhase> expectedTestPhases = new ArrayList<TestPhase>(asList(TestPhase.values()));
//        if (!verifyEnabled) {
//            //exclude verify test phases
//            expectedTestPhases.remove(TestPhase.GLOBAL_VERIFY);
//            expectedTestPhases.remove(TestPhase.LOCAL_VERIFY);
//        }
//
//        return expectedTestPhases;
//    }

//    private class TestPhaseCompleter extends Thread {
//
//        private final ComponentRegistry componentRegistry;
//        private final TestPhaseListeners testPhaseListeners;
//        private final FailureCollector failureCollector;
//
//        private TestPhaseCompleter(ComponentRegistry componentRegistry, TestPhaseListeners testPhaseListeners,
//                                   FailureCollector failureCollector) {
//            super("TestPhaseCompleter");
//
//            this.componentRegistry = componentRegistry;
//            this.testPhaseListeners = testPhaseListeners;
//            this.failureCollector = failureCollector;
//
//            setDaemon(true);
//        }
//
//        @Override
//        public void run() {
//            SimulatorAddress workerAddress = new SimulatorAddress(WORKER, 1, 1, 0);
//
//            for (TestPhase testPhase : TestPhase.values()) {
//                sleepMillis(100);
//                for (TestData testData : componentRegistry.getTests()) {
//                    testPhaseListeners.onCompletion(testData.getTestIndex(), testPhase, workerAddress);
//                }
//            }
//
//            if (finishWorkerLatch != null) {
//                await(finishWorkerLatch);
//                FailureOperation operation = new FailureOperation("Worker finished", WORKER_NORMAL_EXIT, workerAddress, "127.0.0.1",
//                        "127.0.0.1:5701", "workerId", "testId", "stacktrace");
//                failureCollector.notify(operation);
//            }
//        }
//    }
//
//    @Test
//    public void testGetTestPhaseSyncMap() {
//        Map<TestPhase, CountDownLatch> testPhaseSyncMap = getTestPhaseSyncMap(5, true, TestPhase.RUN);
//
//        assertEquals(5, testPhaseSyncMap.get(TestPhase.SETUP).getCount());
//        assertEquals(5, testPhaseSyncMap.get(TestPhase.LOCAL_PREPARE).getCount());
//        assertEquals(5, testPhaseSyncMap.get(TestPhase.GLOBAL_PREPARE).getCount());
//        assertEquals(5, testPhaseSyncMap.get(TestPhase.RUN).getCount());
//        assertEquals(0, testPhaseSyncMap.get(TestPhase.GLOBAL_VERIFY).getCount());
//        assertEquals(0, testPhaseSyncMap.get(TestPhase.LOCAL_VERIFY).getCount());
//        assertEquals(0, testPhaseSyncMap.get(TestPhase.GLOBAL_TEARDOWN).getCount());
//        assertEquals(0, testPhaseSyncMap.get(TestPhase.LOCAL_TEARDOWN).getCount());
//    }
//
//    @Test
//    @SuppressWarnings("all")
//    public void testGetTestPhaseSyncMap_notParallel() {
//        Map<TestPhase, CountDownLatch> testPhaseSyncMap = getTestPhaseSyncMap(5, false, TestPhase.RUN);
//
//        assertNull(testPhaseSyncMap);
//    }
}
