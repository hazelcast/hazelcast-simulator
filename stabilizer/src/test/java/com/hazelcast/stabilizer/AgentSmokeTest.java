package com.hazelcast.stabilizer;

import com.hazelcast.stabilizer.agent.Agent;
import com.hazelcast.stabilizer.agent.workerjvm.WorkerJvmSettings;
import com.hazelcast.stabilizer.common.AgentAddress;
import com.hazelcast.stabilizer.common.AgentsFile;
import com.hazelcast.stabilizer.coordinator.AgentMemberLayout;
import com.hazelcast.stabilizer.coordinator.remoting.AgentsClient;
import com.hazelcast.stabilizer.test.Failure;
import com.hazelcast.stabilizer.test.TestCase;
import com.hazelcast.stabilizer.test.TestContext;
import com.hazelcast.stabilizer.test.TestSuite;
import com.hazelcast.stabilizer.test.annotations.Run;
import com.hazelcast.stabilizer.test.annotations.Setup;
import com.hazelcast.stabilizer.test.annotations.Warmup;
import com.hazelcast.stabilizer.worker.commands.GenericCommand;
import com.hazelcast.stabilizer.worker.commands.InitCommand;
import com.hazelcast.stabilizer.worker.commands.RunCommand;
import com.hazelcast.stabilizer.worker.commands.StopCommand;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@Ignore
public class AgentSmokeTest {

    private AgentsClient client;

    @Before
    public void setUp() throws Exception {
        startAgent();
        client = getClient();
    }

    @Test
    public void testSuccess() throws Exception {
        TestCase testCase = new TestCase();
        testCase.setProperty("class", SuccessTest.class.getName());
        test(testCase);
    }

    @Test
    public void testThrowingFailures() throws Exception {
        TestCase testCase = new TestCase();
        testCase.setProperty("class", FailingTest.class.getName());
        test(testCase);

        cooldown();

        List<Failure> failures = client.getFailures();
        assertEquals("Expected 1 failure!", 1, failures.size());
    }

    private void cooldown() {
        System.out.println("Cooldown");
        Utils.sleepSeconds(10);
        System.out.println("Finished cooldown");
    }

    public void test(TestCase testCase) throws Exception {
        TestSuite testSuite = new TestSuite();
        client.initTestSuite(testSuite);

        spawnWorkers(client);

        InitCommand initTestCommand = new InitCommand(testCase);
        System.out.println("InitTest");
        client.executeOnAllWorkers(initTestCommand);

        System.out.println("setup");
        client.executeOnAllWorkers(new GenericCommand(testCase.id, "setup"));

        System.out.println("localWarmup");
        client.executeOnAllWorkers(new GenericCommand(testCase.id, "localWarmup"));
        client.waitForPhaseCompletion("", testCase.id, "localWarmup");

        System.out.println("globalWarmup");
        client.executeOnAllWorkers(new GenericCommand(testCase.id, "globalWarmup"));
        client.waitForPhaseCompletion("", testCase.id, "globalWarmup");

        System.out.println("run");
        RunCommand runCommand = new RunCommand(testCase.id);
        runCommand.clientOnly = false;
        client.executeOnAllWorkers(runCommand);

        System.out.println("Running for 30 seconds");
        Utils.sleepSeconds(30);
        System.out.println("Finished running");

        System.out.println("stop");
        client.executeOnAllWorkers(new StopCommand(testCase.id));
        client.waitForPhaseCompletion("", testCase.id, "stop");

        System.out.println("localVerify");
        client.executeOnAllWorkers(new GenericCommand(testCase.id, "localVerify"));
        client.waitForPhaseCompletion("", testCase.id, "localVerify");

        System.out.println("globalVerify");
        client.executeOnAllWorkers(new GenericCommand(testCase.id, "globalVerify"));
        client.waitForPhaseCompletion("", testCase.id, "globalVerify");

        System.out.println("globalTeardown");
        client.executeOnAllWorkers(new GenericCommand(testCase.id, "globalTeardown"));
        client.waitForPhaseCompletion("", testCase.id, "globalTeardown");

        System.out.println("localTeardown");
        client.executeOnAllWorkers(new GenericCommand(testCase.id, "localTeardown"));
        client.waitForPhaseCompletion("", testCase.id, "localTeardown");

        System.out.println("Done");
    }

    private void spawnWorkers(AgentsClient client) throws TimeoutException {
        WorkerJvmSettings workerJvmSettings = new WorkerJvmSettings();
        workerJvmSettings.profiler = "";
        workerJvmSettings.vmOptions = "";
        workerJvmSettings.workerStartupTimeout = 60000;
        workerJvmSettings.clientHzConfig = Utils.fileAsText("/java/projects/Hazelcast/hazelcast-stabilizer/dist/src/main/dist/conf/client-hazelcast.xml");
        workerJvmSettings.hzConfig = Utils.fileAsText("/java/projects/Hazelcast/hazelcast-stabilizer/dist/src/main/dist/conf/hazelcast.xml");

        AgentMemberLayout agentLayout = new AgentMemberLayout(workerJvmSettings);
        agentLayout.memberSettings.memberWorkerCount = 1;

        client.spawnWorkers(asList(agentLayout),true);
    }

    private void startAgent() throws InterruptedException {
        new Thread() {
            public void run() {
                try {
                    Agent.main(new String[]{});
                } catch (Throwable t) {
                    t.printStackTrace();
                }
            }
        }.start();

        Thread.sleep(10000);
    }

    private AgentsClient getClient() throws IOException {
        File agentFile = File.createTempFile("agents", "txt");
        agentFile.deleteOnExit();
        Utils.writeText("192.168.1.105", agentFile);
        List<AgentAddress> agentAddresses = AgentsFile.load(agentFile);
        return new AgentsClient(agentAddresses);
    }

    @SuppressWarnings("unused")
    public static class SuccessTest {
        private TestContext context;

        @Run
        void run() {
            while (!context.isStopped()) {
                Utils.sleepSeconds(1);
                System.out.println("Running");
            }
        }

        @Warmup
        void warmup() {
            Utils.sleepSeconds(10);
        }

        @Setup
        void setup(TestContext context) {
            this.context = context;
        }
    }

    @SuppressWarnings("unused")
    public static class FailingTest {
        private TestContext context;

        @Run
        void run() {
            if (!context.isStopped()) {
                System.out.println("Running");
                Utils.sleepSeconds(1);

                System.out.println("Failing");
                throw new RuntimeException("This test should fail");
            }
        }

        @Warmup
        void warmup() {
            Utils.sleepSeconds(10);
        }

        @Setup
        void setup(TestContext context) {
            this.context = context;
        }
    }
}
