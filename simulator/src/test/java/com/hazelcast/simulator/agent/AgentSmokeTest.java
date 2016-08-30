package com.hazelcast.simulator.agent;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.simulator.common.FailureType;
import com.hazelcast.simulator.common.SimulatorProperties;
import com.hazelcast.simulator.common.TestCase;
import com.hazelcast.simulator.common.TestPhase;
import com.hazelcast.simulator.common.TestSuite;
import com.hazelcast.simulator.coordinator.DeploymentPlan;
import com.hazelcast.simulator.coordinator.FailureCollector;
import com.hazelcast.simulator.coordinator.FailureListener;
import com.hazelcast.simulator.coordinator.PerformanceStatsCollector;
import com.hazelcast.simulator.coordinator.RemoteClient;
import com.hazelcast.simulator.coordinator.StartWorkersTask;
import com.hazelcast.simulator.coordinator.TerminateWorkersTask;
import com.hazelcast.simulator.coordinator.TestPhaseListener;
import com.hazelcast.simulator.coordinator.TestPhaseListeners;
import com.hazelcast.simulator.coordinator.WorkerParameters;
import com.hazelcast.simulator.protocol.connector.CoordinatorConnector;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.operation.CreateTestOperation;
import com.hazelcast.simulator.protocol.operation.FailureOperation;
import com.hazelcast.simulator.protocol.operation.InitSessionOperation;
import com.hazelcast.simulator.protocol.operation.StartTestOperation;
import com.hazelcast.simulator.protocol.operation.StartTestPhaseOperation;
import com.hazelcast.simulator.protocol.operation.StopTestOperation;
import com.hazelcast.simulator.protocol.processors.CoordinatorOperationProcessor;
import com.hazelcast.simulator.protocol.registry.ComponentRegistry;
import com.hazelcast.simulator.test.TestException;
import com.hazelcast.simulator.tests.FailingTest;
import com.hazelcast.simulator.tests.SuccessTest;
import com.hazelcast.simulator.utils.AssertTask;
import com.hazelcast.simulator.utils.CommonUtils;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

import static com.hazelcast.simulator.TestEnvironmentUtils.internalDistPath;
import static com.hazelcast.simulator.TestEnvironmentUtils.localResourceDirectory;
import static com.hazelcast.simulator.TestEnvironmentUtils.resetLogLevel;
import static com.hazelcast.simulator.TestEnvironmentUtils.setupFakeEnvironment;
import static com.hazelcast.simulator.TestEnvironmentUtils.tearDownFakeEnvironment;
import static com.hazelcast.simulator.coordinator.DeploymentPlan.createSingleInstanceDeploymentPlan;
import static com.hazelcast.simulator.utils.CommonUtils.await;
import static com.hazelcast.simulator.utils.CommonUtils.closeQuietly;
import static com.hazelcast.simulator.utils.CommonUtils.joinThread;
import static com.hazelcast.simulator.utils.CommonUtils.sleepSeconds;
import static com.hazelcast.simulator.utils.FileUtils.deleteQuiet;
import static com.hazelcast.simulator.utils.FileUtils.fileAsText;
import static com.hazelcast.simulator.utils.TestUtils.assertTrueEventually;
import static com.hazelcast.simulator.utils.TestUtils.createTmpDirectory;
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
    private SimulatorProperties simulatorProperties;

    @Before
    public void before() throws Exception {
        setupFakeEnvironment();

        LOGGER.info("Agent bind address for smoke test: " + AGENT_IP_ADDRESS);
        LOGGER.info("Test runtime for smoke test: " + TEST_RUNTIME_SECONDS + " seconds");

        componentRegistry = new ComponentRegistry();
        componentRegistry.addAgent(AGENT_IP_ADDRESS, AGENT_IP_ADDRESS);

        agentStarter = new AgentStarter();

        simulatorProperties = new SimulatorProperties();

        testPhaseListeners = new TestPhaseListeners();
        PerformanceStatsCollector performanceStatsCollector = new PerformanceStatsCollector();
        outputDirectory = createTmpDirectory();
        failureCollector = new FailureCollector(outputDirectory, componentRegistry);

        CoordinatorOperationProcessor processor = new CoordinatorOperationProcessor(
                null, failureCollector, testPhaseListeners, performanceStatsCollector);
        coordinatorConnector = new CoordinatorConnector(processor, COORDINATOR_PORT);
        coordinatorConnector.addAgent(1, AGENT_IP_ADDRESS, AGENT_PORT);
        coordinatorConnector.start();

        remoteClient = new RemoteClient(coordinatorConnector, componentRegistry, (int) SECONDS.toMillis(10));
        remoteClient.sendToAllAgents(new InitSessionOperation("AgentSmokeTest"));

        failureCollector.addListener(true, new FailureListener() {
            @Override
            public void onFailure(FailureOperation failure, boolean isFinishedFailure, boolean isCritical) {
                FailureType failureType = failure.getType();

                if (failureType.isWorkerFinishedFailure()) {
                    componentRegistry.removeWorker(failure.getWorkerAddress());
                }
            }
        });
    }

    @After
    public void after() {
        try {
            LOGGER.info("Shutdown of CoordinatorConnector...");
            coordinatorConnector.shutdown();
            LOGGER.info("Shutdown of Agent...");
            agentStarter.shutdown();
            LOGGER.info("Finally shutdown agent");
        } finally {
            Hazelcast.shutdownAll();
            resetLogLevel();
        }

        tearDownFakeEnvironment();

        closeQuietly(remoteClient);
        deleteQuiet(outputDirectory);
    }

    @Override
    public void onFailure(FailureOperation failure, boolean isFinishedFailure, boolean isCritical) {
        failureOperations.add(failure);
    }

    @Test(timeout = 300000)
    public void testSuccess() throws Exception {
        TestCase testCase = new TestCase("testSuccess");
        testCase.setProperty("class", SuccessTest.class.getName());
        executeTestCase(testCase);
    }

    @Test(timeout = 300000)
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

    // todo: we don't we use the testsuite runner for this? This is just fragile and duplication
    private void executeTestCase(TestCase testCase) throws Exception {
        try {
            String testId = testCase.getId();
            TestSuite testSuite = new TestSuite();
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
           // componentRegistry.removeTests();

            LOGGER.info("Terminating workers...");
            new TerminateWorkersTask(simulatorProperties, componentRegistry, remoteClient).run();

            LOGGER.info("Testcase done!");
        }
    }

    private void createWorkers() {
        Map<String, String> environment = new ConcurrentHashMap<String, String>();
        environment.put("AUTOCREATE_HAZELCAST_INSTANCE", "true");
        environment.put("LOG4j_CONFIG", fileAsText(internalDistPath() + "/conf/worker-log4j.xml"));
        environment.put("JVM_OPTIONS", "");
        environment.put("WORKER_PERFORMANCE_MONITOR_INTERVAL_SECONDS", "0");
        environment.put("HAZELCAST_CONFIG", fileAsText(localResourceDirectory() + "/hazelcast.xml"));

        WorkerParameters workerParameters = new WorkerParameters(
                "outofthebox",
                60000,
                fileAsText(internalDistPath() + "/conf/worker-hazelcast-member.sh"),
                environment);
        DeploymentPlan deploymentPlan = createSingleInstanceDeploymentPlan(AGENT_IP_ADDRESS, workerParameters);
        new StartWorkersTask(deploymentPlan.getWorkerDeployment(), remoteClient, componentRegistry, 0).run();
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
        public void onCompletion(TestPhase testPhase, SimulatorAddress workerAddress) {
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
