package com.hazelcast.stabilizer.coordinator;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.TestCase;
import com.hazelcast.stabilizer.Utils;
import com.hazelcast.stabilizer.agent.workerjvm.WorkerJvmSettings;
import com.hazelcast.stabilizer.coordinator.remoting.AgentsClient;
import com.hazelcast.stabilizer.probes.probes.ProbesResultXmlWriter;
import com.hazelcast.stabilizer.probes.probes.Result;
import com.hazelcast.stabilizer.tests.Failure;
import com.hazelcast.stabilizer.tests.TestSuite;
import com.hazelcast.stabilizer.tests.map.helpers.StringUtils;
import com.hazelcast.stabilizer.worker.commands.GenericCommand;
import com.hazelcast.stabilizer.worker.commands.GetBenchmarkResultsCommand;
import com.hazelcast.stabilizer.worker.commands.InitCommand;
import com.hazelcast.stabilizer.worker.commands.RunCommand;
import com.hazelcast.stabilizer.worker.commands.StopCommand;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import static com.hazelcast.stabilizer.Utils.secondsToHuman;
import static java.lang.String.format;

/**
 * TestCase runner is responsible for running a single test case. Multiple test-cases can be run in parallel,
 * by having multiple TestCaseRunners  in parallel.
 */
public class TestCaseRunner {
    private final static ILogger log = Logger.getLogger(TestCaseRunner.class);

    private final TestCase testCase;
    private final Coordinator coordinator;
    private final AgentsClient agentsClient;
    private final TestSuite testSuite;
    private final String prefix;
    private final Set<Failure.Type> nonCriticalFailures;
    //private final NumberFormat performanceFormat = NumberFormat.getInstance(Locale.US);

    public TestCaseRunner(TestCase testCase, TestSuite testSuite, Coordinator coordinator, int maxTextCaseIdLength) {
        this.testCase = testCase;
        this.coordinator = coordinator;
        this.testSuite = testSuite;
        this.agentsClient = coordinator.agentsClient;
        this.prefix = (testCase.id.isEmpty() ? "" : Utils.padRight(testCase.id, maxTextCaseIdLength + 1));
        this.nonCriticalFailures = testSuite.tolerableFailures;
    }

    public boolean run() throws Exception {
        log.info("--------------------------------------------------------------\n" +
                format("Running Test : %s\n%s", testCase.getId(), testCase) + "\n" +
                "--------------------------------------------------------------");

        int oldFailureCount = coordinator.failureList.size();
        try {
            echo("Starting Test initialization");
            agentsClient.executeOnAllWorkers(new InitCommand(testCase));
            echo("Completed Test initialization");

            echo("Starting Test setup");
            agentsClient.executeOnAllWorkers(new GenericCommand(testCase.id, "setup"));
            agentsClient.waitForPhaseCompletion(prefix, testCase.id, "setup");
            echo("Completed Test setup");

            echo("Starting Test local warmup");
            agentsClient.executeOnAllWorkers(new GenericCommand(testCase.id, "localWarmup"));
            agentsClient.waitForPhaseCompletion(prefix, testCase.id, "localWarmup");
            echo("Completed Test local warmup");

            echo("Starting Test global warmup");
            agentsClient.executeOnSingleWorker(new GenericCommand(testCase.id, "globalWarmup"));
            agentsClient.waitForPhaseCompletion(prefix, testCase.id, "globalWarmup");
            echo("Completed Test global warmup");

            echo("Starting Test start");
            startTestCase();
            echo("Completed Test start");

            echo(format("Test will run for %s", secondsToHuman(testSuite.duration)));
            sleepSeconds(testSuite.duration);
            echo("Test finished running");

            echo("Starting Test stop");
            agentsClient.executeOnAllWorkers(new StopCommand(testCase.id));
            agentsClient.waitForPhaseCompletion(prefix, testCase.id, "stop");
            echo("Completed Test stop");

            logPerformance();
            processProbeResults();

            if (coordinator.verifyEnabled) {
                echo("Starting Test global verify");
                agentsClient.executeOnSingleWorker(new GenericCommand(testCase.id, "globalVerify"));
                agentsClient.waitForPhaseCompletion(prefix, testCase.id, "globalVerify");
                echo("Completed Test global verify");

                echo("Starting Test local verify");
                agentsClient.executeOnAllWorkers(new GenericCommand(testCase.id, "localVerify"));
                agentsClient.waitForPhaseCompletion(prefix, testCase.id, "localVerify");
                echo("Completed Test local verify");
            } else {
                echo("Skipping Test verification");
            }

            echo("Starting Test global tear down");
            agentsClient.executeOnSingleWorker(new GenericCommand(testCase.id, "globalTeardown"));
            agentsClient.waitForPhaseCompletion(prefix, testCase.id, "globalTeardown");
            echo("Finished Test global tear down");

            echo("Starting Test local tear down");
            agentsClient.waitForPhaseCompletion(prefix, testCase.id, "localTeardown");
            agentsClient.executeOnAllWorkers(new GenericCommand(testCase.id, "localTeardown"));
            echo("Completed Test local tear down");

            return coordinator.failureList.size() == oldFailureCount;
        } catch (Exception e) {
            log.severe("Failed", e);
            return false;
        }
    }

    private void processProbeResults() {
        Map<String, ? extends Result> probesResult = getProbesResult();
        if (!probesResult.isEmpty()) {
            ProbesResultXmlWriter xmlWriter = new ProbesResultXmlWriter();
            xmlWriter.write(probesResult, new File("results-" + coordinator.testSuite.id + ".xml"));
            logProbesResultInHumanReadableFormat(probesResult);
        }
    }

    private <R extends Result<R>> void logProbesResultInHumanReadableFormat(Map<String, R> combinedResults) {
        for (Map.Entry<String, R> entry : combinedResults.entrySet()) {
            String probeName = entry.getKey();
            R result = entry.getValue();
            echo("Probe " + probeName + " result: " + result.toHumanString());
        }
    }

    private <R extends Result<R>> Map<String, R> getProbesResult() {
        Map<String, R> combinedResults = new HashMap<String, R>();
        List<List<Map<String, R>>> agentsProbeResults;
        try {
            agentsProbeResults = agentsClient.executeOnAllWorkers(new GetBenchmarkResultsCommand(testCase.id));
        } catch (TimeoutException e) {
            log.severe("A timeout happened while retrieving the benchmark results");
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
                        } else {
                            log.warning("Probe " + probeName + " has null value for some member. This should not happen.");
                        }
                    }
                }
            }
        }
        return combinedResults;
    }

    private void logPerformance() {
        if (coordinator.monitorPerformance) {
            coordinator.performanceMonitor.logDetailedPerformanceInfo(testSuite.duration);
        }
    }

    private void startTestCase() throws TimeoutException {
        if (coordinator.monitorPerformance) {
            coordinator.performanceMonitor.start();
        }

        WorkerJvmSettings workerJvmSettings = coordinator.workerJvmSettings;
        RunCommand runCommand = new RunCommand(testCase.id);
        runCommand.clientOnly = workerJvmSettings.clientWorkerCount > 0;
        agentsClient.executeOnAllWorkers(runCommand);
    }

    public void sleepSeconds(int seconds) {
        int period = 30;
        int big = seconds / period;
        int small = seconds % period;

        for (int k = 1; k <= big; k++) {
            if (shouldTerminate()) {
                echo("Critical Failure detected, aborting execution of test");
                return;
            }

            Utils.sleepSeconds(period);
            final int elapsed = period * k;
            final float percentage = (100f * elapsed) / seconds;
            String msg = format("Running %s %6.2f%% complete", secondsToHuman(elapsed), percentage);

            if (coordinator.monitorPerformance) {
                if (coordinator.operationCount < 0) {
                    msg += ", performance not available";
                } else {
                    msg += String.format("%s ops/s %s ops",
                            Utils.formatDouble(coordinator.performance, 14),
                            Utils.formatLong(coordinator.operationCount, 14)
                    );
                }
            }

            log.info(prefix + msg);
        }

        Utils.sleepSeconds(small);
    }

    private boolean shouldTerminate() {
        for (Failure failure : coordinator.failureList) {
            if (!nonCriticalFailures.contains(failure.type)) {
                return true;
            }
        }
        return false;
    }

    private void echo(String msg) {
        try {
            agentsClient.echo(prefix + msg);
        } catch (TimeoutException e) {
            log.warning("Failed to echo message due to timeout");
        }
        log.info(prefix + msg);
    }
}
