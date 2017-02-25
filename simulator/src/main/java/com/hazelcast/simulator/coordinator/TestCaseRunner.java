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
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.operation.CreateTestOperation;
import com.hazelcast.simulator.protocol.operation.StartTestOperation;
import com.hazelcast.simulator.protocol.operation.StartTestPhaseOperation;
import com.hazelcast.simulator.protocol.operation.StopTestOperation;
import com.hazelcast.simulator.protocol.registry.ComponentRegistry;
import com.hazelcast.simulator.protocol.registry.TargetType;
import com.hazelcast.simulator.protocol.registry.TestData;
import com.hazelcast.simulator.protocol.registry.WorkerData;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;

import static com.hazelcast.simulator.common.TestPhase.GLOBAL_PREPARE;
import static com.hazelcast.simulator.common.TestPhase.GLOBAL_TEARDOWN;
import static com.hazelcast.simulator.common.TestPhase.GLOBAL_VERIFY;
import static com.hazelcast.simulator.common.TestPhase.LOCAL_PREPARE;
import static com.hazelcast.simulator.common.TestPhase.LOCAL_TEARDOWN;
import static com.hazelcast.simulator.common.TestPhase.LOCAL_VERIFY;
import static com.hazelcast.simulator.common.TestPhase.RUN;
import static com.hazelcast.simulator.common.TestPhase.SETUP;
import static com.hazelcast.simulator.protocol.registry.TestData.CompletedStatus.FAILED;
import static com.hazelcast.simulator.protocol.registry.TestData.CompletedStatus.SUCCESS;
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
import static java.util.Collections.synchronizedList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Responsible for running a single {@link TestCase}.
 * <p>
 * Multiple TestCases can be run in parallel, by having multiple TestCaseRunners in parallel.
 */
public final class TestCaseRunner implements TestPhaseListener {

    private static final int RUN_PHASE_LOG_INTERVAL_SECONDS = 30;
    private static final int WAIT_FOR_PHASE_COMPLETION_LOG_INTERVAL_SECONDS = 30;
    private static final int WAIT_FOR_PHASE_COMPLETION_LOG_VERBOSE_DELAY_SECONDS = 300;

    private static final Logger LOGGER = Logger.getLogger(TestCaseRunner.class);

    private final ConcurrentMap<TestPhase, List<SimulatorAddress>> phaseCompletedMap
            = new ConcurrentHashMap<TestPhase, List<SimulatorAddress>>();

    private final TestData test;
    private final TestCase testCase;
    private final TestSuite testSuite;

    private final RemoteClient remoteClient;
    private final FailureCollector failureCollector;
    private final PerformanceStatsCollector performanceStatsCollector;
    private final ComponentRegistry componentRegistry;

    private final String prefix;
    private final Map<TestPhase, CountDownLatch> testPhaseSyncMap;

    private final boolean isVerifyEnabled;
    private final TargetType targetType;
    private final int targetCount;

    private final int performanceMonitorIntervalSeconds;
    private final int logRunPhaseIntervalSeconds;
    private final List<WorkerData> targets;

    @SuppressWarnings("checkstyle:parameternumber")
    public TestCaseRunner(TestData test,
                          List<WorkerData> targets,
                          RemoteClient remoteClient,
                          Map<TestPhase, CountDownLatch> testPhaseSyncMap,
                          FailureCollector failureCollector,
                          ComponentRegistry componentRegistry,
                          PerformanceStatsCollector performanceStatsCollector,
                          int performanceMonitorIntervalSeconds) {
        this.test = test;
        this.testCase = test.getTestCase();
        this.testSuite = test.getTestSuite();

        this.remoteClient = remoteClient;
        this.failureCollector = failureCollector;
        this.performanceStatsCollector = performanceStatsCollector;
        this.componentRegistry = componentRegistry;

        String testAddress = test.getAddress().toString();
        this.prefix = padRight(testAddress + ":" + testCase.getId()
                , testSuite.getMaxTestCaseIdLength() + 2 + testAddress.length());

        this.testPhaseSyncMap = testPhaseSyncMap;

        this.targets = targets;
        this.isVerifyEnabled = testSuite.isVerifyEnabled();
        this.targetType = testSuite.getWorkerQuery().getTargetType().resolvePreferClient(componentRegistry.hasClientWorkers());
        this.targetCount = targets.size();

        this.performanceMonitorIntervalSeconds = performanceMonitorIntervalSeconds;
        if (performanceMonitorIntervalSeconds > 0) {
            this.logRunPhaseIntervalSeconds = min(performanceMonitorIntervalSeconds, RUN_PHASE_LOG_INTERVAL_SECONDS);
        } else {
            this.logRunPhaseIntervalSeconds = RUN_PHASE_LOG_INTERVAL_SECONDS;
        }

        for (TestPhase testPhase : TestPhase.values()) {
            phaseCompletedMap.put(testPhase, synchronizedList(new ArrayList<SimulatorAddress>()));
        }
    }

    @Override
    public void onCompletion(TestPhase testPhase, SimulatorAddress workerAddress) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("Completed: " + testPhase + " from worker: " + workerAddress);
        }
        phaseCompletedMap.get(testPhase).add(workerAddress);
    }

    public boolean run() {
        logDetails();

        test.initStartTime();
        try {
            run0();
        } catch (TestCaseAbortedException e) {
            echo(e.getMessage());
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
        executePhase(SETUP);

        executePhase(LOCAL_PREPARE);
        executePhase(GLOBAL_PREPARE);

        executeRun();

        if (isVerifyEnabled) {
            executePhase(GLOBAL_VERIFY);
            executePhase(LOCAL_VERIFY);
        } else {
            echo("Skipping Test verification");
        }

        executePhase(GLOBAL_TEARDOWN);
        executePhase(LOCAL_TEARDOWN);
    }

    private void logDetails() {
        LOGGER.info(format("Test %s using %s workers [%s]",
                testCase.getId(), targets.size(), WorkerData.toAddressString(targets)));
    }

    private void createTest() {
        echo("Starting Test initialization");
        remoteClient.invokeOnAllWorkers(new CreateTestOperation(test.getTestIndex(), testCase));
        echo("Completed Test initialization");
    }

    private void executePhase(TestPhase phase) {
        if (hasFailure()) {
            throw new TestCaseAbortedException("Skipping Test " + phase.desc() + " (critical failure)", phase);
        }

        echo("Starting Test " + phase.desc());
        test.setTestPhase(phase);
        if (phase.isGlobal()) {
            remoteClient.invokeOnTestOnFirstWorker(test.getAddress(), new StartTestPhaseOperation(phase));
        } else {
            remoteClient.invokeOnTestOnAllWorkers(test.getAddress(), new StartTestPhaseOperation(phase));
        }

        waitForPhaseCompletion(phase);
        echo("Completed Test " + phase.desc());
        waitForGlobalTestPhaseCompletion(phase);
    }

    @SuppressWarnings("checkstyle:npathcomplexity")
    private void executeRun() {
        if (test.isStopRequested()) {
            echo(format("Skipping %s, test stopped.", RUN));
            return;
        }

        test.setTestPhase(RUN);
        start(RUN);

        long startMs = currentTimeMillis();

        int durationSeconds = testSuite.getDurationSeconds();
        long durationMs;
        long timeoutMs;
        if (durationSeconds == 0) {
            echo(format("Test will %s until it stops", RUN));
            timeoutMs = Long.MAX_VALUE;
            durationMs = Long.MAX_VALUE;
        } else {
            durationMs = SECONDS.toMillis(durationSeconds);
            long warmupSeconds = MILLISECONDS.toSeconds(testCase.getWarmupMillis());
            if (warmupSeconds > 0) {
                echo(format("Test will run for %s with a warmup period of %s",
                        secondsToHuman(durationSeconds),
                        secondsToHuman(warmupSeconds)));
            } else {
                echo(format("Test will run for %s without warmup", secondsToHuman(durationSeconds)));
            }
            timeoutMs = startMs + durationMs;
        }

        long nextSleepUntilMs = startMs;
        int iteration = 0;
        for (; ; ) {
            nextSleepUntilMs += SECONDS.toMillis(1);
            sleepUntilMs(nextSleepUntilMs);

            if (hasFailure()) {
                echo(format("Critical failure detected, aborting %s phase", RUN));
                break;
            }

            long nowMs = currentTimeMillis();
            if (nowMs > timeoutMs || isPhaseCompleted(RUN) || test.isStopRequested()) {
                echo(format("Test finished %s", RUN.desc()));
                break;
            }

            iteration++;
            if (iteration % logRunPhaseIntervalSeconds == 0) {
                logProgress(nowMs - startMs, durationMs);
            }
        }

        stop(RUN);

        logPerformanceInfo();

        waitForGlobalTestPhaseCompletion(RUN);
    }

    private void logPerformanceInfo() {
        long durationMillis = SECONDS.toMillis(testSuite.getDurationSeconds()) - testCase.getWarmupMillis();

        if (performanceMonitorIntervalSeconds > 0) {
            LOGGER.info(testCase.getId() + " Waiting for all performance info");
            sleepSeconds(performanceMonitorIntervalSeconds);

            LOGGER.info("Performance " + testCase.getId() + "\n"
                    + performanceStatsCollector.detailedPerformanceInfo(testCase.getId(), durationMillis));
        }
    }

    private void start(TestPhase phase) {
        echo(format("Starting Test %s start on %s", phase.desc(), targetType.toString(targetCount)));

        List<String> addresses = new LinkedList<String>();
        for (WorkerData workers : targets) {
            addresses.add(workers.getAddress().toString());
        }

        echo(format("Test %s using workers %s", phase.desc(), WorkerData.toAddressString(targets)));

        remoteClient.invokeOnTestOnAllWorkers(
                test.getAddress(),
                new StartTestOperation(targetType, addresses));

        echo(format("Completed Test %s start", phase.desc()));
    }

    public void stop(TestPhase phase) {
        echo(format("Executing Test %s stop", phase.desc()));
        remoteClient.invokeOnTestOnAllWorkers(test.getAddress(), new StopTestOperation());
        try {
            waitForPhaseCompletion(phase);
            echo(format("Completed Test %s stop", phase.desc()));
        } catch (TestCaseAbortedException e) {
            echo(e.getMessage());
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

    private void waitForPhaseCompletion(TestPhase testPhase) {
        int completedWorkers = phaseCompletedMap.get(testPhase).size();
        int expectedWorkers = getExpectedWorkerCount(testPhase);

        long started = System.nanoTime();
        while (completedWorkers < expectedWorkers) {
            sleepSeconds(1);

            if (hasFailure()) {
                throw new TestCaseAbortedException(
                        format("Waiting for %s completion aborted (critical failure)", testPhase.desc()), testPhase);
            }

            completedWorkers = phaseCompletedMap.get(testPhase).size();
            expectedWorkers = getExpectedWorkerCount(testPhase);

            logMissingWorkers(testPhase, completedWorkers, expectedWorkers, started);
        }
    }

    private boolean isPhaseCompleted(TestPhase testPhase) {
        int completedWorkers = phaseCompletedMap.get(testPhase).size();
        int expectedWorkers = getExpectedWorkerCount(testPhase);
        return completedWorkers >= expectedWorkers;
    }

    private void logMissingWorkers(TestPhase testPhase, int completedWorkers, int expectedWorkers, long started) {
        long elapsed = getElapsedSeconds(started);
        if (elapsed % WAIT_FOR_PHASE_COMPLETION_LOG_INTERVAL_SECONDS != 0) {
            return;
        }
        if (elapsed < WAIT_FOR_PHASE_COMPLETION_LOG_VERBOSE_DELAY_SECONDS || completedWorkers == expectedWorkers) {
            echo(format("Waiting %s for %s completion (%d/%d workers)", secondsToHuman(elapsed), testPhase.desc(),
                    completedWorkers, expectedWorkers));
            return;
        }
        // verbose logging of missing workers
        List<SimulatorAddress> missingWorkers = new ArrayList<SimulatorAddress>(expectedWorkers - completedWorkers);
        if (expectedWorkers == 1) {
            missingWorkers.add(componentRegistry.getFirstWorker().getAddress());
        } else {
            for (WorkerData worker : componentRegistry.getWorkers()) {
                if (!phaseCompletedMap.get(testPhase).contains(worker.getAddress())) {
                    missingWorkers.add(worker.getAddress());
                }
            }
        }
        echo(format("Waiting %s for %s completion (%d/%d workers) (missing workers: %s)", secondsToHuman(elapsed),
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

    private int getExpectedWorkerCount(TestPhase testPhase) {
        return testPhase.isGlobal() ? 1 : componentRegistry.workerCount();
    }

    private CountDownLatch decrementAndGetCountDownLatch(TestPhase testPhase) {
        if (testPhaseSyncMap == null) {
            return new CountDownLatch(0);
        }
        CountDownLatch latch = testPhaseSyncMap.get(testPhase);
        latch.countDown();
        return latch;
    }

    private void echo(String msg) {
        remoteClient.logOnAllAgents(prefix + msg);
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
