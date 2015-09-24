package com.hazelcast.simulator.agent;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.simulator.common.JavaProfiler;
import com.hazelcast.simulator.common.SimulatorProperties;
import com.hazelcast.simulator.coordinator.AgentMemberLayout;
import com.hazelcast.simulator.coordinator.AgentMemberMode;
import com.hazelcast.simulator.coordinator.CoordinatorParameters;
import com.hazelcast.simulator.coordinator.PerformanceStateContainer;
import com.hazelcast.simulator.coordinator.remoting.AgentsClient;
import com.hazelcast.simulator.coordinator.remoting.RemoteClient;
import com.hazelcast.simulator.protocol.connector.CoordinatorConnector;
import com.hazelcast.simulator.protocol.operation.CreateTestOperation;
import com.hazelcast.simulator.protocol.operation.StartTestOperation;
import com.hazelcast.simulator.protocol.operation.StartTestPhaseOperation;
import com.hazelcast.simulator.protocol.operation.StopTestOperation;
import com.hazelcast.simulator.protocol.registry.AgentData;
import com.hazelcast.simulator.protocol.registry.ComponentRegistry;
import com.hazelcast.simulator.test.Failure;
import com.hazelcast.simulator.test.TestCase;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;

import static com.hazelcast.simulator.protocol.configuration.Ports.AGENT_PORT;
import static com.hazelcast.simulator.utils.CommonUtils.sleepSeconds;
import static com.hazelcast.simulator.utils.FileUtils.deleteQuiet;
import static com.hazelcast.simulator.utils.FileUtils.fileAsText;
import static com.hazelcast.simulator.utils.TestUtils.assertTrueEventually;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AgentSmokeTest {

    private static final String AGENT_IP_ADDRESS = System.getProperty("agentBindAddress", "127.0.0.1");
    private static final int TEST_RUNTIME_SECONDS = Integer.parseInt(System.getProperty("testRuntimeSeconds", "5"));

    private static final Logger LOGGER = Logger.getLogger(AgentSmokeTest.class);

    private static String userDir;
    private static AgentStarter agentStarter;
    private static AgentsClient agentsClient;

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
        agentStarter.start();

        agentsClient = new AgentsClient(componentRegistry.getAgents());
        agentsClient.start();

        PerformanceStateContainer performanceStateContainer = new PerformanceStateContainer();
        coordinatorConnector = new CoordinatorConnector(performanceStateContainer);
        coordinatorConnector.addAgent(1, AGENT_IP_ADDRESS, AGENT_PORT);

        remoteClient = new RemoteClient(coordinatorConnector, componentRegistry);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        try {
            try {
                try {
                    LOGGER.info("Shutdown of CoordinatorConnector...");
                    coordinatorConnector.shutdown();
                } finally {
                    LOGGER.info("Shutdown of AgentsClient...");
                    agentsClient.shutdown();
                }
            } finally {
                LOGGER.info("Shutdown of Agent...");
                agentStarter.stop();
            }
        } finally {
            Hazelcast.shutdownAll();

            System.setProperty("user.dir", userDir);

            deleteQuiet(new File("./dist/src/main/dist/workers"));
        }
    }

    @Test
    public void testSuccess() throws Exception {
        TestCase testCase = new TestCase("testSuccess");
        testCase.setProperty("class", SuccessTest.class.getName());
        executeTestCase(testCase, 0);
    }

    @Test
    public void testThrowingFailures() throws Exception {
        TestCase testCase = new TestCase("testThrowingFailures");
        testCase.setProperty("class", FailingTest.class.getName());

        List<Failure> failures = executeTestCase(testCase, 2);

        Failure failure = failures.get(0);
        assertEquals("Expected started test to fail", testCase.getId(), failure.testId);
        assertTrue("Expected started test to fail", failure.cause.contains("This test should fail"));
    }

    public List<Failure> executeTestCase(TestCase testCase, int expectedFailures) throws Exception {
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

        final List<Failure> failures = getFailures(expectedFailures);

        LOGGER.info("Terminating workers...");
        remoteClient.terminateWorkers();

        LOGGER.info("Testcase done!");
        return failures;
    }

    private static void createWorkers() {
        AgentData agentData = new AgentData(1, AGENT_IP_ADDRESS, AGENT_IP_ADDRESS);
        AgentMemberLayout agentLayout = new AgentMemberLayout(agentData, AgentMemberMode.MEMBER);
        agentLayout.addWorker(WorkerType.MEMBER, getParameters());

        remoteClient.createWorkers(Collections.singletonList(agentLayout));
    }

    private static CoordinatorParameters getParameters() {
        SimulatorProperties properties = new SimulatorProperties();

        CoordinatorParameters parameters = mock(CoordinatorParameters.class);
        when(parameters.getSimulatorProperties()).thenReturn(properties);
        when(parameters.isAutoCreateHzInstance()).thenReturn(true);
        when(parameters.isPassiveMembers()).thenReturn(false);
        when(parameters.isRefreshJvm()).thenReturn(false);
        when(parameters.isParallel()).thenReturn(true);
        when(parameters.isMonitorPerformance()).thenReturn(true);
        when(parameters.getProfiler()).thenReturn(JavaProfiler.NONE);
        when(parameters.getProfilerSettings()).thenReturn("");
        when(parameters.getNumaCtl()).thenReturn("none");
        when(parameters.getLastTestPhaseToSync()).thenReturn(TestPhase.SETUP);
        when(parameters.getMemberHzConfig()).thenReturn(fileAsText("./simulator/src/test/resources/hazelcast.xml"));
        when(parameters.getClientHzConfig()).thenReturn(fileAsText("./simulator/src/test/resources/client-hazelcast.xml"));
        when(parameters.getLog4jConfig()).thenReturn(fileAsText("./dist/src/main/dist/conf/worker-log4j.xml"));
        when(parameters.getDedicatedMemberMachineCount()).thenReturn(0);
        when(parameters.getMemberWorkerCount()).thenReturn(1);
        when(parameters.getClientWorkerCount()).thenReturn(0);
        when(parameters.getWorkerStartupTimeout()).thenReturn(60000);

        return parameters;
    }

    private static void runPhase(TestCase testCase, TestPhase testPhase) throws TimeoutException {
        LOGGER.info("Starting " + testPhase.desc() + " phase...");
        remoteClient.sendToAllWorkers(new StartTestPhaseOperation(testCase.getId(), testPhase));
        remoteClient.waitForPhaseCompletion("", testCase.getId(), testPhase);
    }

    private static List<Failure> getFailures(final int expectedFailures) {
        final List<Failure> failures = new ArrayList<Failure>();
        if (expectedFailures > 0) {
            assertTrueEventually(new AssertTask() {
                @Override
                public void run() throws Exception {
                    failures.addAll(agentsClient.getFailures());
                    assertEquals("Expected " + expectedFailures + " failures!", expectedFailures, failures.size());
                }
            });
        }
        return failures;
    }

    private static final class AgentStarter {

        private final CountDownLatch latch = new CountDownLatch(1);
        private final AgentThread agentThread = new AgentThread();

        private void start() throws Exception {
            agentThread.start();
            latch.await();
        }

        private void stop() throws Exception {
            agentThread.shutdown();
            agentThread.interrupt();
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

            private void shutdown() {
                agent.shutdown();
            }
        }
    }
}
