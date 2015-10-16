package com.hazelcast.simulator.agent;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.simulator.common.SimulatorProperties;
import com.hazelcast.simulator.coordinator.AgentMemberLayout;
import com.hazelcast.simulator.coordinator.AgentMemberMode;
import com.hazelcast.simulator.coordinator.FailureContainer;
import com.hazelcast.simulator.coordinator.PerformanceStateContainer;
import com.hazelcast.simulator.coordinator.RemoteClient;
import com.hazelcast.simulator.coordinator.TestHistogramContainer;
import com.hazelcast.simulator.coordinator.WorkerParameters;
import com.hazelcast.simulator.protocol.connector.CoordinatorConnector;
import com.hazelcast.simulator.protocol.operation.CreateTestOperation;
import com.hazelcast.simulator.protocol.operation.FailureOperation;
import com.hazelcast.simulator.protocol.operation.StartTestOperation;
import com.hazelcast.simulator.protocol.operation.StartTestPhaseOperation;
import com.hazelcast.simulator.protocol.operation.StopTestOperation;
import com.hazelcast.simulator.protocol.registry.AgentData;
import com.hazelcast.simulator.protocol.registry.ComponentRegistry;
import com.hazelcast.simulator.test.TestCase;
import com.hazelcast.simulator.test.TestException;
import com.hazelcast.simulator.test.TestPhase;
import com.hazelcast.simulator.test.TestSuite;
import com.hazelcast.simulator.tests.FailingTest;
import com.hazelcast.simulator.tests.SuccessTest;
import com.hazelcast.simulator.utils.AssertTask;
import com.hazelcast.simulator.worker.WorkerType;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.Collections;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;

import static com.hazelcast.simulator.protocol.configuration.Ports.AGENT_PORT;
import static com.hazelcast.simulator.utils.CommonUtils.sleepSeconds;
import static com.hazelcast.simulator.utils.FileUtils.deleteQuiet;
import static com.hazelcast.simulator.utils.FileUtils.fileAsText;
import static com.hazelcast.simulator.utils.TestUtils.assertTrueEventually;
import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AgentSmokeTest {

    private static final String AGENT_IP_ADDRESS = System.getProperty("agentBindAddress", "127.0.0.1");
    private static final int TEST_RUNTIME_SECONDS = Integer.parseInt(System.getProperty("testRuntimeSeconds", "5"));

    private static final Logger LOGGER = Logger.getLogger(AgentSmokeTest.class);

    private static String userDir;
    private static AgentStarter agentStarter;

    private static FailureContainer failureContainer;

    private static CoordinatorConnector coordinatorConnector;
    private static RemoteClient remoteClient;

    @BeforeClass
    public static void setUp() throws Exception {
        userDir = System.getProperty("user.dir");

        System.setProperty("user.dir", "./dist/src/main/dist");

        LOGGER.info("Agent bind address for smoke test: " + AGENT_IP_ADDRESS);
        LOGGER.info("Test runtime for smoke test: " + TEST_RUNTIME_SECONDS + " seconds");

        ComponentRegistry componentRegistry = new ComponentRegistry();
        componentRegistry.addAgent(AGENT_IP_ADDRESS, AGENT_IP_ADDRESS);

        agentStarter = new AgentStarter();

        PerformanceStateContainer performanceStateContainer = new PerformanceStateContainer();
        TestHistogramContainer testHistogramContainer = new TestHistogramContainer(performanceStateContainer);
        failureContainer = new FailureContainer("agentSmokeTest");

        coordinatorConnector = new CoordinatorConnector(performanceStateContainer, testHistogramContainer, failureContainer);
        coordinatorConnector.addAgent(1, AGENT_IP_ADDRESS, AGENT_PORT);

        remoteClient = new RemoteClient(coordinatorConnector, componentRegistry);
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

            System.setProperty("user.dir", userDir);

            deleteQuiet(new File("./dist/src/main/dist/workers"));
            deleteQuiet(new File("./logs"));
            deleteQuiet(new File("./workers"));
            deleteQuiet(new File("./failures-agentSmokeTest.txt"));
        }
    }

    @Test
    public void testSuccess() throws Exception {
        TestCase testCase = new TestCase("testSuccess");
        testCase.setProperty("class", SuccessTest.class.getName());
        executeTestCase(testCase);
    }

    @Test
    public void testThrowingFailures() throws Exception {
        TestCase testCase = new TestCase("testThrowingFailures");
        testCase.setProperty("class", FailingTest.class.getName());

        executeTestCase(testCase);

        Queue<FailureOperation> failureOperations = getFailureOperations(2);

        FailureOperation failure = failureOperations.poll();
        assertEquals("Expected test to fail", testCase.getId(), failure.getTestId());
        assertExceptionClassInFailure(failure, TestException.class);

        failure = failureOperations.poll();
        assertEquals("Expected test to fail", testCase.getId(), failure.getTestId());
        assertExceptionClassInFailure(failure, AssertionError.class);
    }

    private void assertExceptionClassInFailure(FailureOperation failure, Class<? extends Throwable> failureClass) {
        assertTrue(format("Expected cause to start with %s, but was %s", failureClass.getCanonicalName(), failure.getCause()),
                failure.getCause().startsWith(failureClass.getCanonicalName()));
    }

    private void executeTestCase(TestCase testCase) throws Exception {
        TestSuite testSuite = new TestSuite();
        remoteClient.initTestSuite(testSuite);

        LOGGER.info("Creating workers...");
        createWorkers();

        LOGGER.info("InitTest phase...");
        remoteClient.sendToAllWorkers(new CreateTestOperation(testCase));

        runPhase(testCase, TestPhase.SETUP);

        runPhase(testCase, TestPhase.LOCAL_WARMUP);
        runPhase(testCase, TestPhase.GLOBAL_WARMUP);

        LOGGER.info("Starting run phase...");
        remoteClient.sendToAllWorkers(new StartTestOperation(testCase.getId(), false));

        LOGGER.info("Running for " + TEST_RUNTIME_SECONDS + " seconds");
        sleepSeconds(TEST_RUNTIME_SECONDS);
        LOGGER.info("Finished running");

        LOGGER.info("Stopping test...");
        remoteClient.sendToAllWorkers(new StopTestOperation(testCase.getId()));
        remoteClient.waitForPhaseCompletion("", testCase.getId(), TestPhase.RUN);

        runPhase(testCase, TestPhase.GLOBAL_VERIFY);
        runPhase(testCase, TestPhase.LOCAL_VERIFY);

        runPhase(testCase, TestPhase.GLOBAL_TEARDOWN);
        runPhase(testCase, TestPhase.LOCAL_TEARDOWN);

        LOGGER.info("Terminating workers...");
        remoteClient.terminateWorkers();

        LOGGER.info("Testcase done!");
    }

    private static void createWorkers() {
        WorkerParameters workerParameters = new WorkerParameters(
                new SimulatorProperties(),
                true,
                60000,
                "",
                "",
                fileAsText("./simulator/src/test/resources/hazelcast.xml"),
                "",
                fileAsText("./dist/src/main/dist/conf/worker-log4j.xml"),
                false
        );

        AgentData agentData = new AgentData(1, AGENT_IP_ADDRESS, AGENT_IP_ADDRESS);
        AgentMemberLayout agentLayout = new AgentMemberLayout(agentData, AgentMemberMode.MEMBER);
        agentLayout.addWorker(WorkerType.MEMBER, workerParameters);

        remoteClient.createWorkers(Collections.singletonList(agentLayout));
    }

    private static void runPhase(TestCase testCase, TestPhase testPhase) throws TimeoutException {
        LOGGER.info("Starting " + testPhase.desc() + " phase...");
        remoteClient.sendToAllWorkers(new StartTestPhaseOperation(testCase.getId(), testPhase));
        remoteClient.waitForPhaseCompletion("", testCase.getId(), testPhase);
    }

    private static Queue<FailureOperation> getFailureOperations(final int expectedFailures) {
        if (expectedFailures > 0) {
            assertTrueEventually(new AssertTask() {
                @Override
                public void run() throws Exception {
                    assertEquals("Expected " + expectedFailures + " failures!", expectedFailures,
                            failureContainer.getFailureCount());
                }
            });
        }
        return failureContainer.getFailureOperations();
    }

    private static final class AgentStarter {

        private final CountDownLatch latch = new CountDownLatch(1);
        private final AgentThread agentThread = new AgentThread();

        public AgentStarter() throws Exception {
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
                        "--publicAddress", "127.0.0.1"
                });
                latch.countDown();
            }

            private void shutdown() throws Exception {
                agent.shutdown();
            }
        }
    }
}
