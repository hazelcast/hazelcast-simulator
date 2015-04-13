package com.hazelcast.simulator.agent;

import com.hazelcast.simulator.agent.workerjvm.WorkerJvmSettings;
import com.hazelcast.simulator.common.AgentAddress;
import com.hazelcast.simulator.common.AgentsFile;
import com.hazelcast.simulator.coordinator.AgentMemberLayout;
import com.hazelcast.simulator.coordinator.Coordinator;
import com.hazelcast.simulator.coordinator.remoting.AgentsClient;
import com.hazelcast.simulator.test.Failure;
import com.hazelcast.simulator.test.TestCase;
import com.hazelcast.simulator.test.TestSuite;
import com.hazelcast.simulator.tests.FailingTest;
import com.hazelcast.simulator.tests.SuccessTest;
import com.hazelcast.simulator.worker.commands.GenericCommand;
import com.hazelcast.simulator.worker.commands.InitCommand;
import com.hazelcast.simulator.worker.commands.RunCommand;
import com.hazelcast.simulator.worker.commands.StopCommand;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static com.hazelcast.simulator.utils.CommonUtils.sleepSeconds;
import static com.hazelcast.simulator.utils.FileUtils.deleteQuiet;
import static com.hazelcast.simulator.utils.FileUtils.fileAsText;
import static com.hazelcast.simulator.utils.FileUtils.writeText;
import static org.junit.Assert.assertEquals;

public class AgentSmokeTest {

    private static final String AGENT_IP_ADDRESS = System.getProperty("agentBindAddress", "127.0.0.1");
    private static final int TEST_RUNTIME_SECONDS = Integer.parseInt(System.getProperty("testRuntimeSeconds", "10"));

    private static final Logger LOGGER = Logger.getLogger(Coordinator.class);

    private static String userDir;
    private static Thread agentThread;
    private static AgentsClient agentsClient;

    @BeforeClass
    public static void setUp() throws Exception {
        userDir = System.getProperty("user.dir");

        System.setProperty("worker.testmethod.timeout", "5");
        System.setProperty("user.dir", "./dist/src/main/dist");

        LOGGER.info("Agent bind address for smoke test: " + AGENT_IP_ADDRESS);
        LOGGER.info("Test runtime for smoke test: " + TEST_RUNTIME_SECONDS + " seconds");

        startAgent();
        agentsClient = getAgentsClient();
        //agentsClient.start();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        agentsClient.stop();

        agentThread.interrupt();
        agentThread.join();

        System.setProperty("user.dir", userDir);
        deleteQuiet(new File("./dist/src/main/dist/workers"));
    }

    @Test
    public void testSuccess() throws Exception {
        TestCase testCase = new TestCase();
        testCase.setProperty("class", SuccessTest.class.getName());
        testCase.id = "testSuccess";
        executeTestCase(testCase);
    }

    @Test
    public void testThrowingFailures() throws Exception {
        TestCase testCase = new TestCase();
        testCase.setProperty("class", FailingTest.class.getName());
        testCase.id = "testThrowingFailures";
        executeTestCase(testCase);

        cooldown();

        List<Failure> failures = agentsClient.getFailures();
        assertEquals("Expected 1 failure!", 1, failures.size());
    }

    private void cooldown() {
        LOGGER.info("Cooldown...");
        sleepSeconds(3);
        LOGGER.info("Finished cooldown");
    }

    public void executeTestCase(TestCase testCase) throws Exception {
        TestSuite testSuite = new TestSuite();
        agentsClient.initTestSuite(testSuite);

        LOGGER.info("Spawning workers...");
        spawnWorkers(agentsClient);

        InitCommand initTestCommand = new InitCommand(testCase);
        LOGGER.info("InitTest phase...");
        agentsClient.executeOnAllWorkers(initTestCommand);

        LOGGER.info("Setup phase...");
        agentsClient.executeOnAllWorkers(new GenericCommand(testCase.id, "setUp"));

        LOGGER.info("Local warmup phase...");
        agentsClient.executeOnAllWorkers(new GenericCommand(testCase.id, "localWarmup"));
        agentsClient.waitForPhaseCompletion("", testCase.id, "localWarmup");

        LOGGER.info("Global warmup phase...");
        agentsClient.executeOnAllWorkers(new GenericCommand(testCase.id, "globalWarmup"));
        agentsClient.waitForPhaseCompletion("", testCase.id, "globalWarmup");

        LOGGER.info("Run phase...");
        RunCommand runCommand = new RunCommand(testCase.id);
        runCommand.clientOnly = false;
        agentsClient.executeOnAllWorkers(runCommand);

        LOGGER.info("Running for " + TEST_RUNTIME_SECONDS + " seconds");
        sleepSeconds(TEST_RUNTIME_SECONDS);
        LOGGER.info("Finished running");

        LOGGER.info("Stopping test...");
        agentsClient.executeOnAllWorkers(new StopCommand(testCase.id));
        agentsClient.waitForPhaseCompletion("", testCase.id, "stop");

        LOGGER.info("Local verify phase...");
        agentsClient.executeOnAllWorkers(new GenericCommand(testCase.id, "localVerify"));
        agentsClient.waitForPhaseCompletion("", testCase.id, "localVerify");

        LOGGER.info("Global verify phase...");
        agentsClient.executeOnAllWorkers(new GenericCommand(testCase.id, "globalVerify"));
        agentsClient.waitForPhaseCompletion("", testCase.id, "globalVerify");

        LOGGER.info("Global teardown phase...");
        agentsClient.executeOnAllWorkers(new GenericCommand(testCase.id, "globalTeardown"));
        agentsClient.waitForPhaseCompletion("", testCase.id, "globalTeardown");

        LOGGER.info("Local teardown phase...");
        agentsClient.executeOnAllWorkers(new GenericCommand(testCase.id, "localTeardown"));
        agentsClient.waitForPhaseCompletion("", testCase.id, "localTeardown");

        LOGGER.info("Testcase done!");
    }

    private void spawnWorkers(AgentsClient client) throws TimeoutException {
        WorkerJvmSettings workerJvmSettings = new WorkerJvmSettings();
        workerJvmSettings.profiler = "";
        workerJvmSettings.vmOptions = "";
        workerJvmSettings.workerStartupTimeout = 60000;
        workerJvmSettings.clientHzConfig = fileAsText("./simulator/src/test/resources/client-hazelcast.xml");
        workerJvmSettings.hzConfig = fileAsText("./simulator/src/test/resources/hazelcast.xml");
        workerJvmSettings.log4jConfig = fileAsText("./simulator/src/test/resources/log4j.xml");

        AgentMemberLayout agentLayout = new AgentMemberLayout(workerJvmSettings);
        agentLayout.memberSettings.memberWorkerCount = 1;
        agentLayout.publicIp = AGENT_IP_ADDRESS;

        client.spawnWorkers(Collections.singletonList(agentLayout), true);
    }

    private static void startAgent() {
        agentThread = new Thread() {
            public void run() {
                try {
                    String[] args = new String[] {
                            "--bindAddress", AGENT_IP_ADDRESS,
                    };
                    Agent.createAgent(args);
                } catch (Throwable t) {
                    t.printStackTrace();
                }
            }
        };
        agentThread.start();
        sleepSeconds(5);
    }

    private static AgentsClient getAgentsClient() throws IOException {
        File agentFile = File.createTempFile("agents", "txt");
        agentFile.deleteOnExit();
        writeText(AGENT_IP_ADDRESS, agentFile);
        List<AgentAddress> agentAddresses = AgentsFile.load(agentFile);
        return new AgentsClient(agentAddresses);
    }
}
