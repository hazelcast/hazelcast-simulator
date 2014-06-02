package com.hazelcast.stabilizer.coordinator;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.TestCase;
import com.hazelcast.stabilizer.Utils;
import com.hazelcast.stabilizer.agent.workerjvm.WorkerJvmSettings;
import com.hazelcast.stabilizer.tests.TestSuite;
import com.hazelcast.stabilizer.worker.testcommands.GenericTestCommand;
import com.hazelcast.stabilizer.worker.testcommands.InitTestCommand;
import com.hazelcast.stabilizer.worker.testcommands.RunCommand;
import com.hazelcast.stabilizer.worker.testcommands.StopTestCommand;

import java.text.NumberFormat;
import java.util.Locale;

import static com.hazelcast.stabilizer.Utils.secondsToHuman;
import static java.lang.String.format;

public class TestCaseRunner {

    private final static ILogger log = Logger.getLogger(TestCaseRunner.class);

    private final TestCase testCase;
    private final Coordinator coordinator;
    private final AgentsClient agentsClient;
    private final TestSuite testSuite;
    private final NumberFormat performanceFormat = NumberFormat.getInstance(Locale.US);

    public TestCaseRunner(TestCase testCase, TestSuite testSuite, Coordinator coordinator) {
        this.testCase = testCase;
        this.coordinator = coordinator;
        this.testSuite = testSuite;
        this.agentsClient = coordinator.agentsClient;
    }

    public boolean run() throws Exception {
        echo("--------------------------------------------------------------");
        echo(format("Running Test : %s\n%s", testCase.getId(), testCase));
        echo("--------------------------------------------------------------");

        int oldFailureCount = coordinator.failureList.size();
        try {
            echo("Starting Test initialization");
            agentsClient.prepareAgentsForTests(testCase);
            agentsClient.executeOnAllWorkers(new InitTestCommand(testCase));
            echo("Completed Test initialization");

            echo("Starting Test setup");
            agentsClient.executeOnAllWorkers(new GenericTestCommand("setup"));
            agentsClient.waitDone();
            echo("Completed Test setup");

            echo("Starting Test local warmup");
            agentsClient.executeOnAllWorkers(new GenericTestCommand("localWarmup"));
            agentsClient.waitDone();
            echo("Completed Test local warmup");

            echo("Starting Test global warmup");
            agentsClient.executeOnSingleWorker(new GenericTestCommand("globalWarmup"));
            agentsClient.waitDone();
            echo("Completed Test global warmup");

            echo("Starting Test start");
            startTestCase();
            echo("Completed Test start");

            echo(format("Test will run for %s", secondsToHuman(testSuite.duration)));
            sleepSeconds(testSuite.duration);
            echo("Test finished running");

            echo("Starting Test stop");
            agentsClient.executeOnAllWorkers(new StopTestCommand());
            echo("Completed Test stop");

            logPerformance();

            if (coordinator.verifyEnabled) {
                echo("Starting Test global verify");
                agentsClient.executeOnSingleWorker(new GenericTestCommand("globalVerify"));
                agentsClient.waitDone();
                echo("Completed Test global verify");

                echo("Starting Test local verify");
                agentsClient.executeOnAllWorkers(new GenericTestCommand("localVerify"));
                agentsClient.waitDone();
                echo("Completed Test local verify");
            } else {
                echo("Skipping Test verification");
            }

            echo("Starting Test global tear down");
            agentsClient.executeOnSingleWorker(new GenericTestCommand("globalTeardown"));
            agentsClient.waitDone();
            echo("Finished Test global tear down");

            echo("Starting Test local tear down");
            agentsClient.waitDone();
            agentsClient.executeOnAllWorkers(new GenericTestCommand("localTeardown"));
            echo("Completed Test local tear down");

            return coordinator.failureList.size() == oldFailureCount;
        } catch (Exception e) {
            log.severe("Failed", e);
            return false;
        }
    }

    private void logPerformance() {
        if (coordinator.monitorPerformance) {
            log.info("Operation-count: " + performanceFormat.format(coordinator.operationCount));
            double performance = (coordinator.operationCount * 1.0d) / testSuite.duration;
            log.info("Performance: " + performanceFormat.format(performance) + " ops/s");
        }
    }

    private void startTestCase() {
        WorkerJvmSettings workerJvmSettings = coordinator.workerJvmSettings;
        RunCommand runCommand = new RunCommand();
        runCommand.clientOnly = workerJvmSettings.mixedWorkerCount > 0 || workerJvmSettings.clientWorkerCount > 0;
        agentsClient.executeOnAllWorkers(runCommand);
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
            String msg = format("Running %s, %-4.2f percent complete", secondsToHuman(elapsed), percentage);

            if (coordinator.monitorPerformance) {
                msg += ", " + performanceFormat.format(coordinator.performance) + " ops/s.";
            }

            log.info(msg);
        }

        Utils.sleepSeconds(small);
    }

    private void echo(String msg) {
        agentsClient.echo(msg);
        log.info(msg);
    }
}
