package com.hazelcast.stabilizer;

import com.hazelcast.stabilizer.agent.Agent;
import com.hazelcast.stabilizer.agent.workerjvm.WorkerJvmSettings;
import com.hazelcast.stabilizer.common.AgentAddress;
import com.hazelcast.stabilizer.common.AgentsFile;
import com.hazelcast.stabilizer.coordinator.remoting.AgentsClient;
import com.hazelcast.stabilizer.tests.Failure;
import com.hazelcast.stabilizer.tests.TestContext;
import com.hazelcast.stabilizer.tests.TestSuite;
import com.hazelcast.stabilizer.tests.annotations.Run;
import com.hazelcast.stabilizer.tests.annotations.Setup;
import com.hazelcast.stabilizer.tests.annotations.Warmup;
import com.hazelcast.stabilizer.tests.map.MapRaceTest;
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
        testCase.setProperty("class", FooTest.class.getName());
        test(testCase);
    }

    @Test
    public void testThrowingFailures() throws Exception {
        TestCase testCase = new TestCase();
        testCase.setProperty("class", MapRaceTest.class.getName());
        test(testCase);

        cooldown();

        List<Failure> failures = client.getFailures();
        assertFalse("No failures found", failures.isEmpty());
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
        client.waitDone("", testCase.id, "localWarmup");

        System.out.println("globalWarmup");
        client.executeOnAllWorkers(new GenericCommand(testCase.id, "globalWarmup"));
        client.waitDone("", testCase.id, "globalWarmup");

        System.out.println("run");
        RunCommand runCommand = new RunCommand(testCase.id);
        runCommand.clientOnly = false;
        client.executeOnAllWorkers(runCommand);

        System.out.println("Running for 30 seconds");
        Utils.sleepSeconds(30);
        System.out.println("Finished running");

        System.out.println("stop");
        client.executeOnAllWorkers(new StopCommand(testCase.id));
        client.waitDone("", testCase.id, "stop");

        System.out.println("localVerify");
        client.executeOnAllWorkers(new GenericCommand(testCase.id, "localVerify"));
        client.waitDone("", testCase.id, "localVerify");

        System.out.println("globalVerify");
        client.executeOnAllWorkers(new GenericCommand(testCase.id, "globalVerify"));
        client.waitDone("", testCase.id, "globalVerify");

        System.out.println("globalTeardown");
        client.executeOnAllWorkers(new GenericCommand(testCase.id, "globalTeardown"));
        client.waitDone("", testCase.id, "globalTeardown");

        System.out.println("localTeardown");
        client.executeOnAllWorkers(new GenericCommand(testCase.id, "localTeardown"));
        client.waitDone("", testCase.id, "localTeardown");

        System.out.println("Done");
    }

    private void spawnWorkers(AgentsClient client) {
        WorkerJvmSettings workerJvmSettings = new WorkerJvmSettings();
        workerJvmSettings.memberWorkerCount = 1;
        workerJvmSettings.profiler = "";
        workerJvmSettings.vmOptions = "";
        workerJvmSettings.workerStartupTimeout = 60000;
        workerJvmSettings.clientHzConfig = Utils.fileAsText("/java/projects/Hazelcast/hazelcast-stabilizer/dist/src/main/dist/conf/client-hazelcast.xml");
        workerJvmSettings.hzConfig = Utils.fileAsText("/java/projects/Hazelcast/hazelcast-stabilizer/dist/src/main/dist/conf/hazelcast.xml");

        client.spawnWorkers(new WorkerJvmSettings[]{workerJvmSettings});
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

    public static class FooTest {
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
}
