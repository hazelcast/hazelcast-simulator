package com.hazelcast.simulator.agent;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.simulator.common.FailureType;
import com.hazelcast.simulator.common.SimulatorProperties;
import com.hazelcast.simulator.common.TestCase;
import com.hazelcast.simulator.common.TestSuite;
import com.hazelcast.simulator.coordinator.FailureCollector;
import com.hazelcast.simulator.coordinator.FailureListener;
import com.hazelcast.simulator.coordinator.PerformanceStatsCollector;
import com.hazelcast.simulator.coordinator.RemoteClient;
import com.hazelcast.simulator.coordinator.StartWorkersTask;
import com.hazelcast.simulator.coordinator.TestPhaseListener;
import com.hazelcast.simulator.coordinator.TestPhaseListeners;
import com.hazelcast.simulator.coordinator.WorkerParameters;
import com.hazelcast.simulator.coordinator.deployment.DeploymentPlan;
import com.hazelcast.simulator.protocol.connector.CoordinatorConnector;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.operation.CreateTestOperation;
import com.hazelcast.simulator.protocol.operation.FailureOperation;
import com.hazelcast.simulator.protocol.operation.InitTestSuiteOperation;
import com.hazelcast.simulator.protocol.operation.StartTestOperation;
import com.hazelcast.simulator.protocol.operation.StartTestPhaseOperation;
import com.hazelcast.simulator.protocol.operation.StopTestOperation;
import com.hazelcast.simulator.protocol.registry.ComponentRegistry;
import com.hazelcast.simulator.test.TestException;
import com.hazelcast.simulator.testcontainer.TestPhase;
import com.hazelcast.simulator.tests.FailingTest;
import com.hazelcast.simulator.tests.SuccessTest;
import com.hazelcast.simulator.utils.AssertTask;
import com.hazelcast.simulator.utils.CommonUtils;
import com.hazelcast.simulator.utils.TestUtils;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.HashSet;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

import static com.hazelcast.simulator.TestEnvironmentUtils.deleteLogs;
import static com.hazelcast.simulator.TestEnvironmentUtils.resetLogLevel;
import static com.hazelcast.simulator.TestEnvironmentUtils.resetUserDir;
import static com.hazelcast.simulator.TestEnvironmentUtils.setDistributionUserDir;
import static com.hazelcast.simulator.coordinator.deployment.DeploymentPlan.createSingleInstanceClusterLayout;
import static com.hazelcast.simulator.utils.CommonUtils.await;
import static com.hazelcast.simulator.utils.CommonUtils.joinThread;
import static com.hazelcast.simulator.utils.CommonUtils.sleepSeconds;
import static com.hazelcast.simulator.utils.FileUtils.deleteQuiet;
import static com.hazelcast.simulator.utils.FileUtils.fileAsText;
import static com.hazelcast.simulator.utils.TestUtils.assertTrueEventually;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AgentSmokeTest implements FailureListener {

    private static final String AGENT_IP_ADDRESS = "127.0.0.1";
    private static final int COORDINATOR_PORT = 0;
    private static final int AGENT_PORT = 10000 + new Random().nextInt(1000);
    private static final int TEST_RUNTIME_SECONDS = 3;

    private static final Logger LOGGER = Logger.getLogger(AgentSmokeTest.class);

    private ComponentRegistry componentRegistry;
    private AgentStarter agentStarter;
    private FailureCollector failureCollector;
    private TestPhaseListeners testPhaseListeners;
    private CoordinatorConnector coordinatorConnector;
    private RemoteClient remoteClient;
    private File outputDirectory;
    private final BlockingQueue<FailureOperation> failureOperations = new LinkedBlockingQueue<FailureOperation>();

    @Before
    public void before() throws Exception {
        setDistributionUserDir();

        LOGGER.info("Agent bind address for smoke test: " + AGENT_IP_ADDRESS);
        LOGGER.info("Test runtime for smoke test: " + TEST_RUNTIME_SECONDS + " seconds");

        componentRegistry = new ComponentRegistry();
        componentRegistry.addAgent(AGENT_IP_ADDRESS, AGENT_IP_ADDRESS);

        agentStarter = new AgentStarter();

        testPhaseListeners = new TestPhaseListeners();
        PerformanceStatsCollector performanceStatsCollector = new PerformanceStatsCollector();
        outputDirectory = TestUtils.createTmpDirectory();
        failureCollector = new FailureCollector(outputDirectory, new HashSet<FailureType>());

        coordinatorConnector = CoordinatorConnector.createInstance(componentRegistry, failureCollector, testPhaseListeners,
                performanceStatsCollector, COORDINATOR_PORT);
        coordinatorConnector.addAgent(1, AGENT_IP_ADDRESS, AGENT_PORT);
        coordinatorConnector.start();

        remoteClient = new RemoteClient(coordinatorConnector, componentRegistry, (int) SECONDS.toMillis(10), 0);
    }

    @After
    public void after() {
        try {
            LOGGER.info("Shutdown of CoordinatorConnector...");
            coordinatorConnector.shutdown();

            LOGGER.info("Shutdown of Agent...");
            agentStarter.shutdown();
        } finally {
            Hazelcast.shutdownAll();

            resetUserDir();
            deleteLogs();

            resetLogLevel();
        }

        deleteQuiet(outputDirectory);
    }

    @Override
    public void onFailure(FailureOperation failure, boolean isFinishedFailure, boolean isCritical) {
        failureOperations.add(failure);
    }

    @Test
    public void testSuccess() throws Exception {

        TestCase testCase = new TestCase("testSuccess");
        testCase.setProperty("class", SuccessTest.class.getName());
        executeTestCase(testCase);
    }

    @Test
    public void testThrowingFailures() throws Exception {
        failureCollector.addListener(this);

        TestCase testCase = new TestCase("testThrowingFailures");
        testCase.setProperty("class", FailingTest.class.getName());

        executeTestCase(testCase);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals("Expected 2 failures!", 2,
                        failureOperations.size());
            }
        });

        FailureOperation failure = failureOperations.poll();
        assertEquals("Expected test to fail", testCase.getId(), failure.getTestId());
        assertExceptionClassInFailure(failure, TestException.class);

        failure = failureOperations.poll();
        assertEquals("Expected test to fail", testCase.getId(), failure.getTestId());
        assertExceptionClassInFailure(failure, AssertionError.class);
    }

    private void executeTestCase(TestCase testCase) throws Exception {
        try {
            String testId = testCase.getId();
            TestSuite testSuite = new TestSuite("AgentSmokeTest-" + testId);
            remoteClient.sendToAllAgents(new InitTestSuiteOperation(testSuite));
            testSuite.addTest(testCase);

            componentRegistry.addTests(testSuite);
            int testIndex = componentRegistry.getTest(testId).getTestIndex();
            LOGGER.info(format("Created TestSuite for %s with index %d", testId, testIndex));

            TestPhaseListenerImpl testPhaseListener = new TestPhaseListenerImpl();
            testPhaseListeners.addListener(testIndex, testPhaseListener);

            LOGGER.info("Creating workers...");
            createWorkers();

            LOGGER.info("InitTest phase...");
            remoteClient.sendToAllWorkers(new CreateTestOperation(testIndex, testCase));

            runPhase(testPhaseListener, testCase, TestPhase.SETUP);

            runPhase(testPhaseListener, testCase, TestPhase.LOCAL_PREPARE);
            runPhase(testPhaseListener, testCase, TestPhase.GLOBAL_PREPARE);

            LOGGER.info("Starting run phase...");
            remoteClient.sendToTestOnAllWorkers(testId, new StartTestOperation());

            LOGGER.info("Running for " + TEST_RUNTIME_SECONDS + " seconds");
            sleepSeconds(TEST_RUNTIME_SECONDS);
            LOGGER.info("Finished running");

            LOGGER.info("Stopping test...");
            remoteClient.sendToTestOnAllWorkers(testId, new StopTestOperation());
            testPhaseListener.await(TestPhase.RUN);

            runPhase(testPhaseListener, testCase, TestPhase.GLOBAL_VERIFY);
            runPhase(testPhaseListener, testCase, TestPhase.LOCAL_VERIFY);

            runPhase(testPhaseListener, testCase, TestPhase.GLOBAL_TEARDOWN);
            runPhase(testPhaseListener, testCase, TestPhase.LOCAL_TEARDOWN);
        } finally {
            componentRegistry.removeTests();

            LOGGER.info("Terminating workers...");
            remoteClient.terminateWorkers(false);

            LOGGER.info("Testcase done!");
        }
    }

    private void createWorkers() {
        WorkerParameters workerParameters = new WorkerParameters(
                new SimulatorProperties(),
                true,
                60000,
                "",
                "",
                fileAsText("simulator/src/test/resources/hazelcast.xml"),
                "",
                fileAsText("dist/src/main/dist/conf/worker-log4j.xml"),
                fileAsText("dist/src/main/dist/conf/worker-hazelcast.sh"),
                false
        );
        DeploymentPlan deploymentPlan = createSingleInstanceClusterLayout(AGENT_IP_ADDRESS, workerParameters);
        new StartWorkersTask(deploymentPlan.asMap(), remoteClient, componentRegistry, 0).run();
    }

    private void runPhase(TestPhaseListenerImpl listener, TestCase testCase, TestPhase testPhase) throws Exception {
        LOGGER.info("Starting " + testPhase.desc() + " phase...");
        if (testPhase.isGlobal()) {
            remoteClient.sendToTestOnFirstWorker(testCase.getId(), new StartTestPhaseOperation(testPhase));
        } else {
            remoteClient.sendToTestOnAllWorkers(testCase.getId(), new StartTestPhaseOperation(testPhase));
        }
        listener.await(testPhase);
    }

    private static void assertExceptionClassInFailure(FailureOperation failure, Class<? extends Throwable> failureClass) {
        assertTrue(format("Expected cause to start with %s, but was %s", failureClass.getCanonicalName(), failure.getCause()),
                failure.getCause().startsWith(failureClass.getCanonicalName()));
    }

    private static final class TestPhaseListenerImpl implements TestPhaseListener {

        private final ConcurrentMap<TestPhase, CountDownLatch> latches = new ConcurrentHashMap<TestPhase, CountDownLatch>();

        private TestPhaseListenerImpl() {
            for (TestPhase testPhase : TestPhase.values()) {
                latches.put(testPhase, new CountDownLatch(1));
            }
        }

        @Override
        public void completed(TestPhase testPhase, SimulatorAddress workerAddress) {
            latches.get(testPhase).countDown();
        }

        private void await(TestPhase testPhase) {
            CommonUtils.await(latches.get(testPhase));
        }
    }

    private static final class AgentStarter {

        private final CountDownLatch latch = new CountDownLatch(1);
        private final AgentThread agentThread = new AgentThread();

        private AgentStarter() {
            agentThread.start();
            await(latch);
        }

        private void shutdown() {
            agentThread.shutdown();
            joinThread(agentThread);
        }

        private class AgentThread extends Thread {

            private Agent agent;

            @Override
            public void run() {
                agent = Agent.startAgent(new String[]{
                        "--addressIndex", "1",
                        "--publicAddress", AGENT_IP_ADDRESS,
                        "--port", String.valueOf(AGENT_PORT)
                });
                latch.countDown();
            }

            private void shutdown() {
                agent.shutdown();
            }
        }
    }
}
