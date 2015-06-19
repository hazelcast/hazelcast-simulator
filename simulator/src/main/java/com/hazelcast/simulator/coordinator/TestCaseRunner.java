package com.hazelcast.simulator.coordinator;

import com.hazelcast.simulator.agent.workerjvm.WorkerJvmSettings;
import com.hazelcast.simulator.coordinator.remoting.AgentsClient;
import com.hazelcast.simulator.probes.probes.ProbesResultXmlWriter;
import com.hazelcast.simulator.probes.probes.Result;
import com.hazelcast.simulator.test.Failure;
import com.hazelcast.simulator.test.TestCase;
import com.hazelcast.simulator.test.TestPhase;
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
 * TestCaseRunner is responsible for running a single TestCase. Multiple TestCases can be run in parallel,
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
    private final int sleepPeriod;

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
        this.sleepPeriod = coordinator.testCaseRunnerSleepPeriod;
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

            runOnAllWorkers(TestPhase.SETUP);

            runOnAllWorkers(TestPhase.LOCAL_WARMUP);
            runOnFirstWorker(TestPhase.GLOBAL_WARMUP);

            startPerformanceMonitor();

            startTestCase();
            if (testSuite.duration > 0) {
                stopTestCaseAfterDuration();
            } else {
                waitForTestCase();
            }

            logPerformance();
            processProbeResults();

            if (coordinator.verifyEnabled) {
                runOnFirstWorker(TestPhase.GLOBAL_VERIFY);
                runOnAllWorkers(TestPhase.LOCAL_VERIFY);
            } else {
                echo("Skipping Test verification");
            }

            runOnFirstWorker(TestPhase.GLOBAL_TEARDOWN);
            runOnAllWorkers(TestPhase.LOCAL_TEARDOWN);

            return (failureMonitor.getFailureCount() == oldFailureCount);
        } catch (Exception e) {
            LOGGER.fatal("Failed", e);
            return false;
        }
    }

    private void runOnAllWorkers(TestPhase testPhase) throws TimeoutException {
        echo("Starting Test " + testPhase.name);
        agentsClient.executeOnAllWorkers(new GenericCommand(testCaseId, testPhase));
        agentsClient.waitForPhaseCompletion(prefix, testCaseId, testPhase);
        echo("Completed Test " + testPhase.name);
    }

    private void runOnFirstWorker(TestPhase testPhase) throws TimeoutException {
        echo("Starting Test " + testPhase.name);
        agentsClient.executeOnFirstWorker(new GenericCommand(testCaseId, testPhase));
        agentsClient.waitForPhaseCompletion(prefix, testCaseId, testPhase);
        echo("Completed Test " + testPhase.name);
    }

    private void startPerformanceMonitor() {
        if (coordinator.monitorPerformance) {
            performanceMonitor.start();
        }
    }

    private void startTestCase() throws TimeoutException {
        echo("Starting Test start");
        WorkerJvmSettings workerJvmSettings = coordinator.workerJvmSettings;
        RunCommand runCommand = new RunCommand(testCaseId);
        runCommand.clientOnly = workerJvmSettings.clientWorkerCount > 0;
        agentsClient.executeOnAllWorkers(runCommand);
        echo("Completed Test start");
    }

    private void stopTestCaseAfterDuration() throws TimeoutException {
        echo(format("Test will run for %s", secondsToHuman(testSuite.duration)));
        sleep(testSuite.duration);
        echo("Test finished running");

        echo("Starting Test stop");
        agentsClient.executeOnAllWorkers(new StopCommand(testCaseId));
        agentsClient.waitForPhaseCompletion(prefix, testCaseId, TestPhase.RUN);
        echo("Completed Test stop");
    }

    private void waitForTestCase() throws TimeoutException {
        echo("Test will run until it stops");
        agentsClient.waitForPhaseCompletion(prefix, testCaseId, TestPhase.RUN);
        echo("Test finished running");
    }

    private void logPerformance() {
        performanceMonitor.logDetailedPerformanceInfo(testSuite.duration);
    }

    private void processProbeResults() {
        Map<String, ? extends Result> probesResult = getProbesResult();
        if (!probesResult.isEmpty()) {
            String fileName = "probes-" + coordinator.testSuite.id + "_" + testCaseId + ".xml";
            ProbesResultXmlWriter.write(probesResult, new File(fileName));
            logProbesResultInHumanReadableFormat(probesResult);
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

    private <R extends Result<R>> void logProbesResultInHumanReadableFormat(Map<String, R> combinedResults) {
        for (Map.Entry<String, R> entry : combinedResults.entrySet()) {
            String probeName = entry.getKey();
            String result = entry.getValue().toHumanString();
            String whitespace = (result.contains("\n") ? "\n" : " ");
            echo("Results of probe " + probeName + ":" + whitespace + result);
        }
    }

    private void sleep(int seconds) {
        int sleepLoops = seconds / sleepPeriod;
        for (int i = 1; i <= sleepLoops; i++) {
            if (failureMonitor.hasCriticalFailure(nonCriticalFailures)) {
                echo("Critical Failure detected, aborting execution of test");
                return;
            }

            sleepSeconds(sleepPeriod);

            int elapsed = sleepPeriod * i;
            float percentage = (100f * elapsed) / seconds;
            String msg = format("Running %s %s%% complete", secondsToHuman(elapsed), formatDouble(percentage, 7));

            if (coordinator.monitorPerformance) {
                msg += performanceMonitor.getPerformanceNumbers();
            }

            LOGGER.info(prefix + msg);
        }

        sleepSeconds(seconds % sleepPeriod);
    }

    private void echo(String msg) {
        agentsClient.echo(prefix + msg);
        LOGGER.info(prefix + msg);
    }
}
