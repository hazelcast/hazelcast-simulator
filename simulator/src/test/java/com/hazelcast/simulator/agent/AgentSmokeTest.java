package com.hazelcast.simulator.agent;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.simulator.cluster.ClusterLayout;
import com.hazelcast.simulator.common.SimulatorProperties;
import com.hazelcast.simulator.coordinator.FailureContainer;
import com.hazelcast.simulator.coordinator.FailureListener;
import com.hazelcast.simulator.coordinator.PerformanceStateContainer;
import com.hazelcast.simulator.coordinator.RemoteClient;
import com.hazelcast.simulator.coordinator.TestHistogramContainer;
import com.hazelcast.simulator.coordinator.TestPhaseListener;
import com.hazelcast.simulator.coordinator.TestPhaseListenerContainer;
import com.hazelcast.simulator.coordinator.WorkerParameters;
import com.hazelcast.simulator.protocol.connector.CoordinatorConnector;
import com.hazelcast.simulator.protocol.operation.CreateTestOperation;
import com.hazelcast.simulator.protocol.operation.FailureOperation;
import com.hazelcast.simulator.protocol.operation.StartTestOperation;
import com.hazelcast.simulator.protocol.operation.StartTestPhaseOperation;
import com.hazelcast.simulator.protocol.operation.StopTestOperation;
import com.hazelcast.simulator.protocol.registry.ComponentRegistry;
import com.hazelcast.simulator.test.TestCase;
import com.hazelcast.simulator.test.TestException;
import com.hazelcast.simulator.test.TestPhase;
import com.hazelcast.simulator.test.TestSuite;
import com.hazelcast.simulator.tests.FailingTest;
import com.hazelcast.simulator.tests.SuccessTest;
import com.hazelcast.simulator.utils.AssertTask;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.simulator.TestEnvironmentUtils.deleteLogs;
import static com.hazelcast.simulator.TestEnvironmentUtils.resetLogLevel;
import static com.hazelcast.simulator.TestEnvironmentUtils.resetUserDir;
import static com.hazelcast.simulator.TestEnvironmentUtils.setDistributionUserDir;
import static com.hazelcast.simulator.TestEnvironmentUtils.setLogLevel;
import static com.hazelcast.simulator.cluster.ClusterLayout.createSingleInstanceClusterLayout;
import static com.hazelcast.simulator.utils.CommonUtils.sleepSeconds;
import static com.hazelcast.simulator.utils.FileUtils.deleteQuiet;
import static com.hazelcast.simulator.utils.FileUtils.fileAsText;
import static com.hazelcast.simulator.utils.TestUtils.assertTrueEventually;
import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AgentSmokeTest implements FailureListener {

    private static final String AGENT_IP_ADDRESS = "127.0.0.1";
    private static final int AGENT_PORT = 10000 + new Random().nextInt(1000);
    private static final int TEST_RUNTIME_SECONDS = 3;

    private static final Logger LOGGER = Logger.getLogger(AgentSmokeTest.class);

    private static ComponentRegistry componentRegistry;
    private static AgentStarter agentStarter;

    private static FailureContainer failureContainer;

    private static TestPhaseListenerContainer testPhaseListenerContainer;
    private static CoordinatorConnector coordinatorConnector;
    private static RemoteClient remoteClient;

    private final BlockingQueue<FailureOperation> failureOperations = new LinkedBlockingQueue<FailureOperation>();

    @BeforeClass
    public static void setUp() throws Exception {
        setLogLevel(Level.TRACE);
        setDistributionUserDir();

        LOGGER.info("Agent bind address for smoke test: " + AGENT_IP_ADDRESS);
        LOGGER.info("Test runtime for smoke test: " + TEST_RUNTIME_SECONDS + " seconds");

        componentRegistry = new ComponentRegistry();
        componentRegistry.addAgent(AGENT_IP_ADDRESS, AGENT_IP_ADDRESS);

        agentStarter = new AgentStarter();

        testPhaseListenerContainer = new TestPhaseListenerContainer();
        PerformanceStateContainer performanceStateContainer = new PerformanceStateContainer();
        TestHistogramContainer testHistogramContainer = new TestHistogramContainer(performanceStateContainer);
        failureContainer = new FailureContainer("agentSmokeTest", null);

        coordinatorConnector = new CoordinatorConnector(failureContainer, testPhaseListenerContainer, performanceStateContainer,
                testHistogramContainer);
        coordinatorConnector.addAgent(1, AGENT_IP_ADDRESS, AGENT_PORT);

        remoteClient = new RemoteClient(coordinatorConnector, componentRegistry, (int) TimeUnit.SECONDS.toMillis(10), 0);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        try {
            LOGGER.info("Shutdown of CoordinatorConnector...");
            coordinatorConnector.shutdown();

            LOGGER.info("Shutdown of Agent...");
            agentStarter.shutdown();
        } finally {
            Hazelcast.shutdownAll();

            resetUserDir();
            deleteLogs();
            deleteQuiet("failures-agentSmokeTest.txt");

            resetLogLevel();
        }
    }

    @Override
    public void onFailure(FailureOperation operation) {
        failureOperations.add(operation);
    }

    @Test
    public void testSuccess() throws Exception {
        TestCase testCase = new TestCase("testSuccess");
        testCase.setProperty("class", SuccessTest.class.getName());
        executeTestCase(testCase);
    }

    @Test
    public void testThrowingFailures() throws Exception {
        failureContainer.addListener(this);

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
            TestSuite testSuite = new TestSuite();
            remoteClient.initTestSuite(testSuite);
            testSuite.addTest(testCase);

            componentRegistry.addTests(testSuite);
            int testIndex = componentRegistry.getTest(testId).getTestIndex();
            LOGGER.info(format("Created TestSuite for %s with index %d", testId, testIndex));

            TestPhaseListenerImpl testPhaseListener = new TestPhaseListenerImpl();
            testPhaseListenerContainer.addListener(testIndex, testPhaseListener);

            LOGGER.info("Creating workers...");
            createWorkers();

            LOGGER.info("InitTest phase...");
            remoteClient.sendToAllWorkers(new CreateTestOperation(testIndex, testCase));

            runPhase(testPhaseListener, testCase, TestPhase.SETUP);

            runPhase(testPhaseListener, testCase, TestPhase.LOCAL_WARMUP);
            runPhase(testPhaseListener, testCase, TestPhase.GLOBAL_WARMUP);

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

    private static void createWorkers() {
        WorkerParameters workerParameters = new WorkerParameters(
                new SimulatorProperties(),
                true,
                60000,
                "",
                "",
                fileAsText("simulator/src/test/resources/hazelcast.xml"),
                "",
                fileAsText("dist/src/main/dist/conf/worker-log4j.xml"),
                false
        );
        ClusterLayout clusterLayout = createSingleInstanceClusterLayout(AGENT_IP_ADDRESS, workerParameters);
        remoteClient.createWorkers(clusterLayout, false);
    }

    private static void runPhase(TestPhaseListenerImpl listener, TestCase testCase, TestPhase testPhase) throws Exception {
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
        public void completed(TestPhase testPhase) {
            latches.get(testPhase).countDown();
        }

        private void await(TestPhase testPhase) throws Exception {
            latches.get(testPhase).await();
        }
    }

    private static final class AgentStarter {

        private final CountDownLatch latch = new CountDownLatch(1);
        private final AgentThread agentThread = new AgentThread();

        private AgentStarter() throws Exception {
            agentThread.start();
            latch.await();
        }

        private void shutdown() throws Exception {
            agentThread.shutdown();
            agentThread.join();
        }

        private class AgentThread extends Thread {

            private Agent agent;

            @Override
            public void run() {
                agent = Agent.createAgent(new String[]{
                        "--addressIndex", "1",
                        "--publicAddress", AGENT_IP_ADDRESS,
                        "--port", String.valueOf(AGENT_PORT)
                });
                latch.countDown();
            }

            private void shutdown() throws Exception {
                agent.shutdown();
            }
        }
    }
}
