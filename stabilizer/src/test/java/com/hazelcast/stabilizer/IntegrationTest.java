package com.hazelcast.stabilizer;

import com.hazelcast.stabilizer.agent.Agent;
import com.hazelcast.stabilizer.agent.workerjvm.WorkerJvmSettings;
import com.hazelcast.stabilizer.coordinator.AgentsClient;
import com.hazelcast.stabilizer.tests.TestContext;
import com.hazelcast.stabilizer.tests.TestSuite;
import com.hazelcast.stabilizer.tests.annotations.Run;
import com.hazelcast.stabilizer.tests.annotations.Setup;
import com.hazelcast.stabilizer.worker.testcommands.DoneCommand;
import com.hazelcast.stabilizer.worker.testcommands.GenericTestCommand;
import com.hazelcast.stabilizer.worker.testcommands.InitTestCommand;
import com.hazelcast.stabilizer.worker.testcommands.RunCommand;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class IntegrationTest {

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
        client.executeOnAllWorkers(initTestCommand);

        client.executeOnAllWorkers(new GenericTestCommand("setup"));

        RunCommand runCommand = new RunCommand();
        runCommand.clientOnly = false;
        client.executeOnAllWorkers(runCommand);

        waitDone(client);
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

    private void waitDone(AgentsClient client) {
        for (; ; ) {
            List<List<Boolean>> result = client.executeOnAllWorkers(new DoneCommand());
            boolean complete = true;
            for (List<Boolean> l : result) {
                for (Boolean b : l) {
                    if (!b) {
                        complete = false;
                        break;
                    }
                }
            }

            if (complete) return;
            System.out.println("Not done");
            Utils.sleepSeconds(1);
        }
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
        @Run
        void run() {
            Utils.sleepSeconds(10);
        }

        @Setup
        void setup(TestContext context) {

        }
    }
}
