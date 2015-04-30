package com.hazelcast.simulator.coordinator;

import com.hazelcast.simulator.agent.workerjvm.WorkerJvmSettings;
import com.hazelcast.simulator.coordinator.remoting.AgentsClient;
import com.hazelcast.simulator.probes.probes.ProbesResultXmlWriter;
import com.hazelcast.simulator.probes.probes.Result;
import com.hazelcast.simulator.test.Failure;
import com.hazelcast.simulator.test.TestCase;
import com.hazelcast.simulator.test.TestSuite;
import com.hazelcast.simulator.worker.commands.GenericCommand;
import com.hazelcast.simulator.worker.commands.GetBenchmarkResultsCommand;
import com.hazelcast.simulator.worker.commands.InitCommand;
import com.hazelcast.simulator.worker.commands.RunCommand;
import com.hazelcast.simulator.worker.commands.StopCommand;
import org.apache.log4j.Logger;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import static com.hazelcast.simulator.utils.CommonUtils.formatDouble;
import static com.hazelcast.simulator.utils.CommonUtils.padRight;
import static com.hazelcast.simulator.utils.CommonUtils.secondsToHuman;
import static com.hazelcast.simulator.utils.CommonUtils.sleepSeconds;
import static java.lang.String.format;

/**
 * TestCase runner is responsible for running a single test case. Multiple test-cases can be run in parallel,
 * by having multiple TestCaseRunners in parallel.
 */
final class TestCaseRunner {

    private static final Logger LOGGER = Logger.getLogger(TestCaseRunner.class);

    private final TestCase testCase;
    private final TestSuite testSuite;
    private final Coordinator coordinator;
    private final AgentsClient agentsClient;
    private final FailureMonitor failureMonitor;
    private final PerformanceMonitor performanceMonitor;
    private final Set<Failure.Type> nonCriticalFailures;
    private final String testCaseId;
    private final String prefix;

    TestCaseRunner(TestCase testCase, TestSuite testSuite, Coordinator coordinator, int maxTextCaseIdLength) {
        this.testCase = testCase;
        this.testSuite = testSuite;
        this.coordinator = coordinator;
        this.agentsClient = coordinator.agentsClient;
        this.failureMonitor = coordinator.failureMonitor;
        this.performanceMonitor = coordinator.performanceMonitor;
        this.nonCriticalFailures = testSuite.tolerableFailures;
        this.testCaseId = testCase.getId();
        this.prefix = (testCaseId.isEmpty() ? "" : padRight(testCaseId, maxTextCaseIdLength + 1));
    }

    boolean run() {
        LOGGER.info("--------------------------------------------------------------\n"
                + format("Running Test: %s%n%s%n", testCaseId, testCase)
                + "--------------------------------------------------------------");

        int oldFailureCount = failureMonitor.getFailureCount();
        try {
            echo("Starting Test initialization");
            agentsClient.executeOnAllWorkers(new InitCommand(testCase));
            echo("Completed Test initialization");

            echo("Starting Test setup");
            agentsClient.executeOnAllWorkers(new GenericCommand(testCaseId, "setUp"));
            agentsClient.waitForPhaseCompletion(prefix, testCaseId, "setUp");
            echo("Completed Test setup");

            echo("Starting Test local warmup");
            agentsClient.executeOnAllWorkers(new GenericCommand(testCaseId, "localWarmup"));
            agentsClient.waitForPhaseCompletion(prefix, testCaseId, "localWarmup");
            echo("Completed Test local warmup");

            echo("Starting Test global warmup");
            agentsClient.executeOnFirstWorker(new GenericCommand(testCaseId, "globalWarmup"));
            agentsClient.waitForPhaseCompletion(prefix, testCaseId, "globalWarmup");
            echo("Completed Test global warmup");

            echo("Starting Test start");
            startTestCase();
            echo("Completed Test start");

            echo(format("Test will run for %s", secondsToHuman(testSuite.duration)));
            sleep(testSuite.duration);
            echo("Test finished running");

            echo("Starting Test stop");
            agentsClient.executeOnAllWorkers(new StopCommand(testCaseId));
            agentsClient.waitForPhaseCompletion(prefix, testCaseId, "stop");
            echo("Completed Test stop");

            logPerformance();
            processProbeResults();

            if (coordinator.verifyEnabled) {
                echo("Starting Test global verify");
                agentsClient.executeOnFirstWorker(new GenericCommand(testCaseId, "globalVerify"));
                agentsClient.waitForPhaseCompletion(prefix, testCaseId, "globalVerify");
                echo("Completed Test global verify");

                echo("Starting Test local verify");
                agentsClient.executeOnAllWorkers(new GenericCommand(testCaseId, "localVerify"));
                agentsClient.waitForPhaseCompletion(prefix, testCaseId, "localVerify");
                echo("Completed Test local verify");
            } else {
                echo("Skipping Test verification");
            }

            echo("Starting Test global tear down");
            agentsClient.executeOnFirstWorker(new GenericCommand(testCaseId, "globalTeardown"));
            agentsClient.waitForPhaseCompletion(prefix, testCaseId, "globalTeardown");
            echo("Finished Test global tear down");

            echo("Starting Test local tear down");
            agentsClient.waitForPhaseCompletion(prefix, testCaseId, "localTeardown");
            agentsClient.executeOnAllWorkers(new GenericCommand(testCaseId, "localTeardown"));
            echo("Completed Test local tear down");

            return (failureMonitor.getFailureCount() == oldFailureCount);
        } catch (Exception e) {
            LOGGER.fatal("Failed", e);
            return false;
        }
    }

    private void processProbeResults() {
        Map<String, ? extends Result> probesResult = getProbesResult();
        if (!probesResult.isEmpty()) {
            String fileName = "probes-" + coordinator.testSuite.id + "_" + testCaseId + ".xml";
            ProbesResultXmlWriter.write(probesResult, new File(fileName));
            logProbesResultInHumanReadableFormat(probesResult);
        }
    }

    private <R extends Result<R>> void logProbesResultInHumanReadableFormat(Map<String, R> combinedResults) {
        for (Map.Entry<String, R> entry : combinedResults.entrySet()) {
            String probeName = entry.getKey();
            String result = entry.getValue().toHumanString();
            String whitespace = (result.contains("\n") ? "\n" : " ");
            echo("Results of probe " + probeName + ":" + whitespace + result);
        }
    }

    private <R extends Result<R>> Map<String, R> getProbesResult() {
        Map<String, R> combinedResults = new HashMap<String, R>();
        List<List<Map<String, R>>> agentsProbeResults;
        try {
            agentsProbeResults = agentsClient.executeOnAllWorkers(new GetBenchmarkResultsCommand(testCaseId));
        } catch (TimeoutException e) {
            LOGGER.fatal("A timeout happened while retrieving the benchmark results");
            return combinedResults;
        }
        for (List<Map<String, R>> agentProbeResults : agentsProbeResults) {
            for (Map<String, R> workerProbeResult : agentProbeResults) {
                if (workerProbeResult != null) {
                    for (Map.Entry<String, R> probe : workerProbeResult.entrySet()) {
                        String probeName = probe.getKey();
                        R currentResult = probe.getValue();
                        if (currentResult != null) {
                            R combinedValue = combinedResults.get(probeName);
                            combinedValue = currentResult.combine(combinedValue);
                            combinedResults.put(probeName, combinedValue);
                        }
                    }
                }
            }
        }
        return combinedResults;
    }

    private void logPerformance() {
        performanceMonitor.logDetailedPerformanceInfo(testSuite.duration);
    }

    private void startTestCase() throws TimeoutException {
        if (coordinator.monitorPerformance) {
            performanceMonitor.start();
        }

        WorkerJvmSettings workerJvmSettings = coordinator.workerJvmSettings;
        RunCommand runCommand = new RunCommand(testCaseId);
        runCommand.clientOnly = workerJvmSettings.clientWorkerCount > 0;
        agentsClient.executeOnAllWorkers(runCommand);
    }

    private void sleep(int seconds) {
        int period = 30;
        int big = seconds / period;
        int small = seconds % period;

        for (int i = 1; i <= big; i++) {
            if (failureMonitor.hasCriticalFailure(nonCriticalFailures)) {
                echo("Critical Failure detected, aborting execution of test");
                return;
            }

            sleepSeconds(period);
            final int elapsed = period * i;
            final float percentage = (100f * elapsed) / seconds;
            String msg = format("Running %s %s%% complete", secondsToHuman(elapsed), formatDouble(percentage, 7));

            if (coordinator.monitorPerformance) {
                msg += performanceMonitor.getPerformanceNumbers();
            }

            LOGGER.info(prefix + msg);
        }

        sleepSeconds(small);
    }

    private void echo(String msg) {
        agentsClient.echo(prefix + msg);
        LOGGER.info(prefix + msg);
    }
}
