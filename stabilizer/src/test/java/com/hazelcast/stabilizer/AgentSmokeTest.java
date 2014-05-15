package com.hazelcast.stabilizer;

import com.hazelcast.stabilizer.agent.Agent;
import com.hazelcast.stabilizer.agent.workerjvm.WorkerJvmSettings;
import com.hazelcast.stabilizer.coordinator.AgentsClient;
import com.hazelcast.stabilizer.tests.TestContext;
import com.hazelcast.stabilizer.tests.TestSuite;
import com.hazelcast.stabilizer.tests.annotations.Run;
import com.hazelcast.stabilizer.tests.annotations.Setup;
import com.hazelcast.stabilizer.tests.annotations.Warmup;
import com.hazelcast.stabilizer.worker.testcommands.DoneCommand;
import com.hazelcast.stabilizer.worker.testcommands.GenericTestCommand;
import com.hazelcast.stabilizer.worker.testcommands.InitTestCommand;
import com.hazelcast.stabilizer.worker.testcommands.RunCommand;
import com.hazelcast.stabilizer.worker.testcommands.StopTestCommand;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class AgentSmokeTest {

    private AgentsClient client;

    @Before
    public void setUp() throws Exception {
        startAgent();
        client = getClient();
    }

    @Test
    public void test() throws Exception {
        TestSuite testSuite = new TestSuite();
        client.initTestSuite(testSuite);

        spawnWorkers(client);

        TestCase testCase = new TestCase();
        testCase.setProperty("class", FooTest.class.getName());
        client.prepareAgentsForTests(testCase);

        InitTestCommand initTestCommand = new InitTestCommand(testCase);
        System.out.println("InitTest");
        client.executeOnAllWorkers(initTestCommand);

        System.out.println("setup");
        client.executeOnAllWorkers(new GenericTestCommand("setup"));

        System.out.println("localWarmup");
        client.executeOnAllWorkers(new GenericTestCommand("localWarmup"));
        client.waitDone();

        System.out.println("globalWarmup");
        client.executeOnAllWorkers(new GenericTestCommand("globalWarmup"));
        client.waitDone();

        System.out.println("run");
        RunCommand runCommand = new RunCommand();
        runCommand.clientOnly = false;
        client.executeOnAllWorkers(runCommand);

        Utils.sleepSeconds(30);

        System.out.println("stop");
        client.executeOnAllWorkers(new StopTestCommand());
        client.waitDone();

        System.out.println("localVerify");
        client.executeOnAllWorkers(new GenericTestCommand("localVerify"));
        client.waitDone();

        System.out.println("globalVerify");
        client.executeOnAllWorkers(new GenericTestCommand("globalVerify"));
        client.waitDone();

        System.out.println("globalTeardown");
        client.executeOnAllWorkers(new GenericTestCommand("globalTeardown"));
        client.waitDone();

        System.out.println("localTeardown");
        client.executeOnAllWorkers(new GenericTestCommand("localTeardown"));
        client.waitDone();

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
        return new AgentsClient(agentFile);
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
