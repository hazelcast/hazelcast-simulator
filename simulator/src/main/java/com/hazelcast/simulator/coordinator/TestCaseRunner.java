/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.simulator.coordinator;

import com.hazelcast.simulator.common.TestCase;
import com.hazelcast.simulator.common.TestPhase;
import com.hazelcast.simulator.coordinator.registry.Registry;
import com.hazelcast.simulator.coordinator.registry.TestData;
import com.hazelcast.simulator.coordinator.registry.WorkerData;
import com.hazelcast.simulator.protocol.CoordinatorClient;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.operation.SimulatorOperation;
import com.hazelcast.simulator.worker.operations.CreateTestOperation;
import com.hazelcast.simulator.worker.operations.StartPhaseOperation;
import com.hazelcast.simulator.worker.operations.StopRunOperation;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.hazelcast.simulator.common.TestPhase.GLOBAL_PREPARE;
import static com.hazelcast.simulator.common.TestPhase.GLOBAL_TEARDOWN;
import static com.hazelcast.simulator.common.TestPhase.GLOBAL_VERIFY;
import static com.hazelcast.simulator.common.TestPhase.LOCAL_PREPARE;
import static com.hazelcast.simulator.common.TestPhase.LOCAL_TEARDOWN;
import static com.hazelcast.simulator.common.TestPhase.LOCAL_VERIFY;
import static com.hazelcast.simulator.common.TestPhase.RUN;
import static com.hazelcast.simulator.common.TestPhase.SETUP;
import static com.hazelcast.simulator.coordinator.registry.TestData.CompletedStatus.FAILED;
import static com.hazelcast.simulator.coordinator.registry.TestData.CompletedStatus.SUCCESS;
import static com.hazelcast.simulator.utils.CommonUtils.await;
import static com.hazelcast.simulator.utils.CommonUtils.getElapsedSeconds;
import static com.hazelcast.simulator.utils.CommonUtils.rethrow;
import static com.hazelcast.simulator.utils.CommonUtils.sleepSeconds;
import static com.hazelcast.simulator.utils.CommonUtils.sleepUntilMs;
import static com.hazelcast.simulator.utils.FormatUtils.formatPercentage;
import static com.hazelcast.simulator.utils.FormatUtils.padRight;
import static com.hazelcast.simulator.utils.FormatUtils.secondsToHuman;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Responsible for running a single {@link TestCase}.
 * <p>
 * Multiple TestCases can be run in parallel, by having multiple TestCaseRunners in parallel.
 */
public final class TestCaseRunner {

    private static final int RUN_PHASE_LOG_INTERVAL_SECONDS = 30;
    private static final int WAIT_FOR_PHASE_COMPLETION_LOG_INTERVAL_SECONDS = 30;
    private static final int WAIT_FOR_PHASE_COMPLETION_LOG_VERBOSE_DELAY_SECONDS = 300;

    private static final Logger LOGGER = Logger.getLogger(TestCaseRunner.class);

    private final TestData test;
    private final TestCase testCase;
    private final TestSuite testSuite;

    private final CoordinatorClient client;
    private final FailureCollector failureCollector;
    private final PerformanceStatsCollector performanceStatsCollector;

    private final String prefix;
    private final Map<TestPhase, CountDownLatch> testPhaseSyncMap;

    private final boolean isVerifyEnabled;
    private final TargetType targetType;
    private final int targetCount;

    private final int performanceMonitorIntervalSeconds;
    private final int logRunPhaseIntervalSeconds;
    private final List<WorkerData> targets;
    private final WorkerData globalTarget;

    @SuppressWarnings("checkstyle:parameternumber")
    public TestCaseRunner(TestData test,
                          List<WorkerData> targets,
                          CoordinatorClient client,
                          Map<TestPhase, CountDownLatch> testPhaseSyncMap,
                          FailureCollector failureCollector,
                          Registry registry,
                          PerformanceStatsCollector performanceStatsCollector,
                          int performanceMonitorIntervalSeconds) {
        this.test = test;
        this.testCase = test.getTestCase();
        this.testSuite = test.getTestSuite();

        this.client = client;
        this.failureCollector = failureCollector;
        this.performanceStatsCollector = performanceStatsCollector;

        this.prefix = padRight(testCase.getId(), testSuite.getMaxTestCaseIdLength() + 1);

        this.testPhaseSyncMap = testPhaseSyncMap;

        this.targets = targets;
        this.globalTarget = targets.iterator().next();
        this.isVerifyEnabled = testSuite.isVerifyEnabled();
        this.targetType = testSuite.getWorkerQuery().getTargetType().resolvePreferClient(registry.hasClientWorkers());
        this.targetCount = targets.size();

        this.performanceMonitorIntervalSeconds = performanceMonitorIntervalSeconds;
        if (performanceMonitorIntervalSeconds > 0) {
            this.logRunPhaseIntervalSeconds = min(performanceMonitorIntervalSeconds, RUN_PHASE_LOG_INTERVAL_SECONDS);
        } else {
            this.logRunPhaseIntervalSeconds = RUN_PHASE_LOG_INTERVAL_SECONDS;
        }
    }

    public boolean run() {
        logDetails();

        test.initStartTime();
        try {
            run0();
        } catch (TestCaseAbortedException e) {
            log(e.getMessage());
            // unblock other TestCaseRunner threads, if fail fast is not set and they have no failures on their own
            TestPhase testPhase = e.testPhase;
            while (testPhase != null) {
                decrementAndGetCountDownLatch(testPhase);
                testPhase = testPhase.getNextTestPhaseOrNull();
            }
        } catch (Exception e) {
            throw rethrow(e);
        } finally {
            test.setCompletedStatus(hasFailure() ? FAILED : SUCCESS);
        }

        return test.getCompletedStatus() == SUCCESS;
    }

    private void run0() {
        createTest();

        LOGGER.info(format("Worker for global test phases will be %s (%s)",
                globalTarget.getAddress(), globalTarget.getParameters().getWorkerType()));

        executePhase(SETUP);
        executePhase(LOCAL_PREPARE);
        executePhase(GLOBAL_PREPARE);
        executeRun();
        if (isVerifyEnabled) {
            executePhase(GLOBAL_VERIFY);
            executePhase(LOCAL_VERIFY);
        } else {
            log("Skipping Test verification");
        }
        executePhase(GLOBAL_TEARDOWN);
        executePhase(LOCAL_TEARDOWN);
    }

    private void logDetails() {
        LOGGER.info(format("Test %s using %s workers [%s]",
                testCase.getId(), targets.size(), WorkerData.toAddressString(targets)));
    }

    private void createTest() {
        log("Starting Test initialization");
        invokeOnTargets(new CreateTestOperation(testCase));
        log("Completed Test initialization");
    }

    private void invokeOnTargets(SimulatorOperation op) {
        Map<WorkerData, Future> futures = submitToTargets(false, op);
        awaitCompletion(futures);
    }

    private Map<WorkerData, Future> submitToTargets(boolean singleTarget, SimulatorOperation op) {
        Map<WorkerData, Future> futures = new HashMap<WorkerData, Future>();

        if (singleTarget) {
            Future f = client.submit(globalTarget.getAddress(), op);
            futures.put(globalTarget, f);
        } else {
            for (WorkerData worker : targets) {
                Future f = client.submit(worker.getAddress(), op);
                futures.put(worker, f);
            }
        }

        return futures;
    }

    private void awaitCompletion(Map<WorkerData, Future> futures) {
        for (Map.Entry<WorkerData, Future> entry : futures.entrySet()) {
            Future f = entry.getValue();
            try {
                f.get();
            } catch (InterruptedException e) {
                throw new RuntimeException();
            } catch (ExecutionException e) {
                throw new RuntimeException(e.getCause());
            }
        }
    }

    private void executePhase(TestPhase phase) {
        if (hasFailure()) {
            throw new TestCaseAbortedException("Skipping Test " + phase.desc() + " (critical failure)", phase);
        }

        log("Starting Test " + phase.desc());
        test.setTestPhase(phase);

        Map<WorkerData, Future> futures = submitToTargets(
                phase.isGlobal(), new StartPhaseOperation(phase, testCase.getId()));

        waitForPhaseCompletion(phase, futures);
        log("Completed Test " + phase.desc());
        waitForGlobalTestPhaseCompletion(phase);
    }

    @SuppressWarnings("checkstyle:npathcomplexity")
    private void executeRun() {
        if (test.isStopRequested()) {
            log(format("Skipping %s, test stopped.", RUN));
            return;
        }

        test.setTestPhase(RUN);
        Map<WorkerData, Future> futures = startRun();

        long startMs = currentTimeMillis();

        long durationSeconds = testSuite.getDurationSeconds();
        long durationMs;
        long timeoutMs;
        if (durationSeconds == 0) {
            log("Test will run until it stops");
            timeoutMs = Long.MAX_VALUE;
            durationMs = Long.MAX_VALUE;
        } else {
            durationMs = SECONDS.toMillis(durationSeconds);
            long warmupSeconds = MILLISECONDS.toSeconds(testCase.getWarmupMillis());
            if (warmupSeconds > 0) {
                log(format("Test will run for %s with a warmup period of %s",
                        secondsToHuman(durationSeconds),
                        secondsToHuman(warmupSeconds)));
            } else {
                log(format("Test will run for %s without warmup", secondsToHuman(durationSeconds)));
            }
            timeoutMs = startMs + durationMs;
        }

        long nextSleepUntilMs = startMs;
        int iteration = 0;
        for (; ; ) {
            nextSleepUntilMs += SECONDS.toMillis(1);
            sleepUntilMs(nextSleepUntilMs);

            if (hasFailure()) {
                log("Critical failure detected, aborting RUN phase");
                break;
            }

            long nowMs = currentTimeMillis();
            if (nowMs > timeoutMs || isAllDone(futures) || test.isStopRequested()) {
                log("Test finished run");
                break;
            }

            iteration++;
            if (iteration % logRunPhaseIntervalSeconds == 0) {
                logProgress(nowMs - startMs, durationMs);
            }
        }

        stopRun();

        waitForPhaseCompletion(RUN, futures);

        logFinalPerformanceInfo(startMs);

        waitForGlobalTestPhaseCompletion(RUN);
    }

    private boolean isAllDone(Map<WorkerData, Future> futures) {
        for (Future f : futures.values()) {
            if (!f.isDone()) {
                return false;
            }
        }

        return true;
    }

    private void logFinalPerformanceInfo(long startMs) {
        // the running time of the test is current time minus the start time. We can't rely on testsuite duration
        // due to premature abortion of a test. Or if the test has no explicit duration configured
        long durationWithWarmupMillis = currentTimeMillis() - startMs;

        // then we need to subtract the warmup.
        long durationMillis = durationWithWarmupMillis - testCase.getWarmupMillis();

        if (performanceMonitorIntervalSeconds > 0) {
            LOGGER.info(testCase.getId() + " Waiting for all performance info");
            sleepSeconds(performanceMonitorIntervalSeconds);

            LOGGER.info("Performance " + testCase.getId() + "\n"
                    + performanceStatsCollector.detailedPerformanceInfo(testCase.getId(), durationMillis));
        }
    }

    /**
     * Starts running the test. This call is asynchronous. It will not wait for the running to complete. It will
     * return a map of futures (one for each target worker) that can be used to sync on completion.
     */
    private Map<WorkerData, Future> startRun() {
        log(format("Starting run on %s workers", targetType.toString(targetCount)));
        log(format("Test run using workers %s", WorkerData.toAddressString(targets)));
        return submitToTargets(false, new StartPhaseOperation(RUN, testCase.getId()));
    }

    private void stopRun() {
        log("Stopping test");

        Map<WorkerData, Future> futures = submitToTargets(false, new StopRunOperation(testCase.getId()));

        try {
            waitForPhaseCompletion(RUN, futures);
            log("Stopping test completed");
        } catch (TestCaseAbortedException e) {
            log(e.getMessage());
        }
    }

    private void logProgress(long elapsedMs, long durationMs) {
        String msg;
        if (durationMs == Long.MAX_VALUE) {
            msg = format("Running %s", secondsToHuman(MILLISECONDS.toSeconds(elapsedMs)));
        } else {
            msg = format("Running %s (%s%%)",
                    secondsToHuman(MILLISECONDS.toSeconds(elapsedMs)),
                    formatPercentage(elapsedMs, durationMs));
        }

        if (performanceMonitorIntervalSeconds > 0) {
            msg += performanceStatsCollector.formatIntervalPerformanceNumbers(testCase.getId());
        }

        LOGGER.info(prefix + msg);
    }

    private void waitForPhaseCompletion(TestPhase testPhase, Map<WorkerData, Future> futures) {
        int completedWorkers = 0;
        int expectedWorkers = futures.size();

        long started = System.nanoTime();
        while (completedWorkers < expectedWorkers) {
            sleepSeconds(1);

            if (hasFailure()) {
                throw new TestCaseAbortedException(
                        format("Waiting for %s completion aborted (critical failure)", testPhase.desc()), testPhase);
            }

            completedWorkers = 0;
            for (Future f : futures.values()) {
                if (f.isDone()) {
                    completedWorkers++;
                }
            }

            logMissingWorkers(testPhase, completedWorkers, expectedWorkers, started, futures);
        }
    }

    private void logMissingWorkers(TestPhase testPhase, int completedWorkers, int expectedWorkers,
                                   long started, Map<WorkerData, Future> futures) {
        long elapsed = getElapsedSeconds(started);
        if (elapsed % WAIT_FOR_PHASE_COMPLETION_LOG_INTERVAL_SECONDS != 0) {
            return;
        }

        if (elapsed < WAIT_FOR_PHASE_COMPLETION_LOG_VERBOSE_DELAY_SECONDS || completedWorkers == expectedWorkers) {
            log(format("Waiting %s for %s completion (%d/%d workers)", secondsToHuman(elapsed), testPhase.desc(),
                    completedWorkers, expectedWorkers));
            return;
        }

        // verbose logging of missing workers
        List<SimulatorAddress> missingWorkers = new ArrayList<SimulatorAddress>();
        for (Map.Entry<WorkerData, Future> entry : futures.entrySet()) {
            if (!entry.getValue().isDone()) {
                missingWorkers.add(entry.getKey().getAddress());
            }
        }
        log(format("Waiting %s for %s completion (%d/%d workers) (missing workers: %s)", secondsToHuman(elapsed),
                testPhase.desc(), completedWorkers, expectedWorkers, missingWorkers));
    }

    private void waitForGlobalTestPhaseCompletion(TestPhase testPhase) {
        if (testPhaseSyncMap == null) {
            return;
        }

        CountDownLatch latch = decrementAndGetCountDownLatch(testPhase);
        if (!hasFailure()) {
            await(latch);
        }

        LOGGER.info(testCase.getId() + " completed waiting for global TestPhase " + testPhase.desc());
    }

    private CountDownLatch decrementAndGetCountDownLatch(TestPhase testPhase) {
        if (testPhaseSyncMap == null) {
            return new CountDownLatch(0);
        }
        CountDownLatch latch = testPhaseSyncMap.get(testPhase);
        latch.countDown();
        return latch;
    }

    private void log(String msg) {
        LOGGER.info(prefix + msg);
    }

    private boolean hasFailure() {
        return failureCollector.hasCriticalFailure(testCase.getId())
                || failureCollector.hasCriticalFailure() && testSuite.isFailFast();
    }

    private static final class TestCaseAbortedException extends RuntimeException {

        private TestPhase testPhase;

        TestCaseAbortedException(String message, TestPhase testPhase) {
            super(message);
            this.testPhase = testPhase;
        }
    }
}
