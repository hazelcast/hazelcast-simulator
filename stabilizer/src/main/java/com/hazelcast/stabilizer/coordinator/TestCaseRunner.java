package com.hazelcast.stabilizer.coordinator;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.TestCase;
import com.hazelcast.stabilizer.Utils;
import com.hazelcast.stabilizer.agent.workerjvm.WorkerJvmSettings;
import com.hazelcast.stabilizer.performance.Performance;
import com.hazelcast.stabilizer.tests.TestSuite;
import com.hazelcast.stabilizer.worker.testcommands.GenericTestCommand;
import com.hazelcast.stabilizer.worker.testcommands.InitTestCommand;
import com.hazelcast.stabilizer.worker.testcommands.StartTestCommand;
import com.hazelcast.stabilizer.worker.testcommands.StopTestCommand;

import static java.lang.String.format;

public class TestCaseRunner {

    private final static ILogger log = Logger.getLogger(TestCaseRunner.class);

    private final TestCase testCase;
    private final Coordinator coordinator;
    private final AgentClientManager agentClientManager;
    private final TestSuite testSuite;

    public TestCaseRunner(TestCase testCase, TestSuite testSuite, Coordinator coordinator) {
        this.testCase = testCase;
        this.coordinator = coordinator;
        this.testSuite = testSuite;
        this.agentClientManager = coordinator.agentClientManager;
    }

    public boolean run() throws Exception {
        echo(format("Running Test : %s", testCase.getId()));

        int oldCount = coordinator.failureList.size();
        try {
            echo(testCase.toString());

            echo("Starting Test initialization");
            agentClientManager.prepareAgentsForTests(testCase);
            agentClientManager.executeOnAllWorkers(new InitTestCommand(testCase));
            echo("Completed Test initialization");

            echo("Starting Test local setup");
            agentClientManager.executeOnAllWorkers(new GenericTestCommand("localSetup"));
            echo("Completed Test local setup");

            echo("Starting Test global setup");
            agentClientManager.executeOnSingleWorker(new GenericTestCommand("globalSetup"));
            echo("Completed Test global setup");

            echo("Starting Test start");
            startTestCase();
            echo("Completed Test start");

            echo(format("Test running for %s", Utils.secondsToHuman(testSuite.duration)));
            sleepSeconds(testSuite.duration);
            echo("Test finished running");

            echo("Starting Test stop");
            agentClientManager.executeOnAllWorkers(new StopTestCommand());
            echo("Completed Test stop");

            if (coordinator.monitorPerformance) {
                echo(calcPerformance().toHumanString());
            }

            if (coordinator.verifyEnabled) {
                echo("Starting Test global verify");
                agentClientManager.executeOnSingleWorker(new GenericTestCommand("globalVerify"));
                echo("Completed Test global verify");

                echo("Starting Test local verify");
                agentClientManager.executeOnAllWorkers(new GenericTestCommand("localVerify"));
                echo("Completed Test local verify");
            } else {
                echo("Skipping Test verification");
            }

            echo("Starting Test global tear down");
            agentClientManager.executeOnSingleWorker(new GenericTestCommand("globalTearDown"));
            echo("Finished Test global tear down");

            echo("Starting Test local tear down");
            agentClientManager.executeOnAllWorkers(new GenericTestCommand("localTearDown"));

            echo("Completed Test local tear down");

            return coordinator.failureList.size() == oldCount;
        } catch (Exception e) {
            log.severe("Failed", e);
            return false;
        }
    }

    private void startTestCase() {
        WorkerJvmSettings workerJvmSettings = coordinator.workerJvmSettings;
        StartTestCommand startTestCommand = new StartTestCommand();
        startTestCommand.clientOnly = workerJvmSettings.mixedWorkerCount > 0 || workerJvmSettings.clientWorkerCount > 0;
        agentClientManager.executeOnAllWorkers(startTestCommand);
    }

    public void sleepSeconds(int seconds) {
        int period = 30;
        int big = seconds / period;
        int small = seconds % period;

        for (int k = 1; k <= big; k++) {
            if (coordinator.failureList.size() > 0) {
                echo("Failure detected, aborting execution of test");
                return;
            }

            Utils.sleepSeconds(period);
            final int elapsed = period * k;
            final float percentage = (100f * elapsed) / seconds;
            String msg = format("Running %s of %s seconds %-4.2f percent complete", elapsed, seconds, percentage);
            echo(msg);
            if (coordinator.monitorPerformance) {
                echo(calcPerformance().toHumanString());
            }
        }

        Utils.sleepSeconds(small);
    }

    private void echo(String msg) {
        agentClientManager.echo(msg);
        log.info(msg);
    }

    public Performance calcPerformance() {
        return null;
//        ShoutToWorkersTask task = new ShoutToWorkersTask(new GenericTestTask("calcPerformance"), "calcPerformance");
//        Map<Member, Future<List<Performance>>> result = agentExecutor.submitToAllMembers(task);
//        Performance performance = null;
//        for (Future<List<Performance>> future : result.values()) {
//            try {
//                List<Performance> results = future.get();
//                for (Performance p : results) {
//                    if (performance == null) {
//                        performance = p;
//                    } else {
//                        performance = performance.merge(p);
//                    }
//                }
//            } catch (InterruptedException e) {
//            } catch (ExecutionException e) {
//                log.severe(e);
//            }
//        }
//        return performance == null ? new NotAvailable() : performance;
    }
}
