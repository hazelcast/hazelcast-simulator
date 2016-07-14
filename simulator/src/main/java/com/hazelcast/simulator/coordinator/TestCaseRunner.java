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

import com.hazelcast.simulator.protocol.operation.CreateTestOperation;
import com.hazelcast.simulator.protocol.operation.StartTestOperation;
import com.hazelcast.simulator.protocol.operation.StartTestPhaseOperation;
import com.hazelcast.simulator.protocol.operation.StopTestOperation;
import com.hazelcast.simulator.protocol.registry.ComponentRegistry;
import com.hazelcast.simulator.protocol.registry.TargetType;
import com.hazelcast.simulator.test.TestCase;
import com.hazelcast.simulator.test.TestPhase;
import com.hazelcast.simulator.test.TestSuite;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.simulator.test.TestPhase.GLOBAL_RESET;
import static com.hazelcast.simulator.test.TestPhase.GLOBAL_TEARDOWN;
import static com.hazelcast.simulator.test.TestPhase.GLOBAL_VERIFY;
import static com.hazelcast.simulator.test.TestPhase.GLOBAL_WARMUP;
import static com.hazelcast.simulator.test.TestPhase.LOCAL_RESET;
import static com.hazelcast.simulator.test.TestPhase.LOCAL_TEARDOWN;
import static com.hazelcast.simulator.test.TestPhase.LOCAL_VERIFY;
import static com.hazelcast.simulator.test.TestPhase.LOCAL_WARMUP;
import static com.hazelcast.simulator.test.TestPhase.RUN;
import static com.hazelcast.simulator.test.TestPhase.SETUP;
import static com.hazelcast.simulator.test.TestPhase.WARMUP;
import static com.hazelcast.simulator.utils.CommonUtils.await;
import static com.hazelcast.simulator.utils.CommonUtils.getElapsedSeconds;
import static com.hazelcast.simulator.utils.CommonUtils.rethrow;
import static com.hazelcast.simulator.utils.CommonUtils.sleepSeconds;
import static com.hazelcast.simulator.utils.FormatUtils.formatPercentage;
import static com.hazelcast.simulator.utils.FormatUtils.padRight;
import static com.hazelcast.simulator.utils.FormatUtils.secondsToHuman;
import static java.lang.String.format;

/**
 * Responsible for running a single {@link TestCase}.
 *
 * Multiple TestCases can be run in parallel, by having multiple TestCaseRunners in parallel.
 */
final class TestCaseRunner implements TestPhaseListener {

    private static final int RUN_PHASE_LOG_INTERVAL_SECONDS = 30;
    private static final int WAIT_FOR_PHASE_COMPLETION_LOG_INTERVAL_SECONDS = 30;

    private static final Logger LOGGER = Logger.getLogger(TestCaseRunner.class);
    private static final ConcurrentMap<TestPhase, Object> LOG_TEST_PHASE_COMPLETION = new ConcurrentHashMap<TestPhase, Object>();

    private final ConcurrentMap<TestPhase, AtomicInteger> phaseCompletedMap = new ConcurrentHashMap<TestPhase, AtomicInteger>();
    private final CountDownLatch waitForStopThread = new CountDownLatch(1);

    private final int testIndex;
    private final TestCase testCase;
    private final String testCaseId;
    private final TestSuite testSuite;

    private final RemoteClient remoteClient;
    private final FailureContainer failureContainer;
    private final PerformanceStateContainer performanceStateContainer;
    private final ComponentRegistry componentRegistry;

    private final String prefix;
    private final Map<TestPhase, CountDownLatch> testPhaseSyncMap;

    private final boolean isVerifyEnabled;
    private final TargetType targetType;
    private final int targetCount;

    private final boolean monitorPerformance;
    private final int logPerformanceIntervalSeconds;
    private final int logRunPhaseIntervalSeconds;

    TestCaseRunner(int testIndex, TestCase testCase, Coordinator coordinator, int paddingLength,
                   Map<TestPhase, CountDownLatch> testPhaseSyncMap) {
        this.testIndex = testIndex;
        this.testCase = testCase;
        this.testCaseId = testCase.getId();
        this.testSuite = coordinator.getTestSuite();

        this.remoteClient = coordinator.getRemoteClient();
        this.failureContainer = coordinator.getFailureContainer();
        this.performanceStateContainer = coordinator.getPerformanceStateContainer();
        this.componentRegistry = coordinator.getComponentRegistry();

        this.prefix = padRight(testCaseId, paddingLength + 1);
        this.testPhaseSyncMap = testPhaseSyncMap;

        CoordinatorParameters coordinatorParameters = coordinator.getCoordinatorParameters();
        this.isVerifyEnabled = coordinatorParameters.isVerifyEnabled();
        this.targetType = coordinatorParameters.getTargetType(componentRegistry.hasClientWorkers());
        this.targetCount = coordinatorParameters.getTargetCount();

        WorkerParameters workerParameters = coordinator.getWorkerParameters();
        this.monitorPerformance = workerParameters.isMonitorPerformance();
        this.logPerformanceIntervalSeconds = workerParameters.getWorkerPerformanceMonitorIntervalSeconds();
        this.logRunPhaseIntervalSeconds = workerParameters.getRunPhaseLogIntervalSeconds(RUN_PHASE_LOG_INTERVAL_SECONDS);

        for (TestPhase testPhase : TestPhase.values()) {
            phaseCompletedMap.put(testPhase, new AtomicInteger());
        }
    }

    @Override
    public void completed(TestPhase testPhase) {
        phaseCompletedMap.get(testPhase).incrementAndGet();
    }

    void run() {
        try {
            createTest();

            executePhase(SETUP);

            executePhase(LOCAL_WARMUP);
            executePhase(GLOBAL_WARMUP);

            if (testSuite.getWarmupDurationSeconds() > 0) {
                executeWarmup();

                executePhase(LOCAL_RESET);
                executePhase(GLOBAL_RESET);
            }else{
                echo("Skipping Test warmup");
            }

            executeRun();

            if (isVerifyEnabled) {
                executePhase(GLOBAL_VERIFY);
                executePhase(LOCAL_VERIFY);
            } else {
                echo("Skipping Test verification");
            }

            executePhase(GLOBAL_TEARDOWN);
            executePhase(LOCAL_TEARDOWN);
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    private void createTest() {
        echo("Starting Test initialization");
        remoteClient.sendToAllWorkers(new CreateTestOperation(testIndex, testCase));
        echo("Completed Test initialization");
    }

    private void executePhase(TestPhase testPhase) {
        if (testSuite.isFailFast() && failureContainer.hasCriticalFailure(testCaseId)) {
            echo("Skipping Test " + testPhase.desc() + " (critical failure)");
            decrementAndGetCountDownLatch(testPhase);
            return;
        }

        echo("Starting Test " + testPhase.desc());
        if (testPhase.isGlobal()) {
            remoteClient.sendToTestOnFirstWorker(testCaseId, new StartTestPhaseOperation(testPhase));
        } else {
            remoteClient.sendToTestOnAllWorkers(testCaseId, new StartTestPhaseOperation(testPhase));
        }
        waitForPhaseCompletion(testPhase);
        echo("Completed Test " + testPhase.desc());
        waitForGlobalTestPhaseCompletion(testPhase);
    }

    private void executeRun() throws Exception {
        echo(format("Starting Test start on %s", targetType.toString(targetCount)));
        List<String> targetWorkers = componentRegistry.getWorkerAddresses(targetType, targetCount);
        remoteClient.sendToTestOnAllWorkers(testCaseId, new StartTestOperation(targetType, targetWorkers));
        echo("Completed Test start");

        StopThread stopThread = null;
        if (testSuite.getDurationSeconds() > 0) {
            stopThread = new StopThread();
            stopThread.start();
        }

        if (testSuite.isWaitForTestCase()) {
            echo("Test will run until it stops");
            waitForPhaseCompletion(RUN);
            echo("Test finished running");

            if (stopThread != null) {
                stopThread.shutdown();
                stopThread.interrupt();
            }
        } else {
            waitForStopThread.await();
        }

        waitForGlobalTestPhaseCompletion(RUN);
    }

    private void executeWarmup() throws Exception {
        echo(format("Starting Test warmup start on %s", targetType.toString(targetCount)));
        List<String> targetWorkers = componentRegistry.getWorkerAddresses(targetType, targetCount);
        remoteClient.sendToTestOnAllWorkers(testCaseId, new StartTestOperation(targetType, targetWorkers));
        echo("Completed Test warmup start");

        StopThread stopThread = null;
        if (testSuite.getDurationSeconds() > 0) {
            stopThread = new StopThread();
            stopThread.start();
        }

        if (testSuite.isWaitForTestCase()) {
            echo("Test will run until it stops");
            waitForPhaseCompletion(WARMUP);
            echo("Test finished running");

            if (stopThread != null) {
                stopThread.shutdown();
                stopThread.interrupt();
            }
        } else {
            waitForStopThread.await();
        }

        waitForGlobalTestPhaseCompletion(WARMUP);
    }

    private void waitForPhaseCompletion(TestPhase testPhase) {
        int completedWorkers = phaseCompletedMap.get(testPhase).get();
        int expectedWorkers = getExpectedWorkerCount(testPhase);

        long started = System.nanoTime();
        while (completedWorkers < expectedWorkers) {
            sleepSeconds(1);

            if (failureContainer.hasCriticalFailure(testCaseId)) {
                echo(format("Waiting for %s completion aborted (critical failure)", testPhase.desc()));
                break;
            }

            completedWorkers = phaseCompletedMap.get(testPhase).get();
            expectedWorkers = getExpectedWorkerCount(testPhase);

            long elapsed = getElapsedSeconds(started);
            if (elapsed % WAIT_FOR_PHASE_COMPLETION_LOG_INTERVAL_SECONDS == 0) {
                echo(format("Waiting %s for %s completion (%d/%d workers)", secondsToHuman(elapsed), testPhase.desc(),
                        completedWorkers, expectedWorkers));
            }
        }
    }

    private void waitForGlobalTestPhaseCompletion(TestPhase testPhase) {
        if (testPhaseSyncMap == null) {
            return;
        }
        CountDownLatch latch = decrementAndGetCountDownLatch(testPhase);
        await(latch);
        if (LOG_TEST_PHASE_COMPLETION.putIfAbsent(testPhase, true) == null) {
            LOGGER.info("Completed TestPhase " + testPhase.desc());
        }
    }

    private int getExpectedWorkerCount(TestPhase testPhase) {
        return (testPhase.isGlobal()) ? 1 : componentRegistry.workerCount();
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

    private final class StopThread extends Thread {

        private volatile boolean isRunning = true;

        public void shutdown() {
            isRunning = false;
        }

        @Override
        public void run() {
            try {
                echo(format("Test will run for %s", secondsToHuman(testSuite.getDurationSeconds())));
                sleepUntilFailure(testSuite.getDurationSeconds());
                echo("Test finished running");

                echo("Starting Test stop");
                remoteClient.sendToTestOnAllWorkers(testCaseId, new StopTestOperation());
                waitForPhaseCompletion(RUN);
                echo("Completed Test stop");
            } finally {
                waitForStopThread.countDown();
            }
        }

        private void sleepUntilFailure(int sleepSeconds) {
            int sleepLoops = sleepSeconds / logRunPhaseIntervalSeconds;
            for (int i = 1; i <= sleepLoops && isRunning; i++) {
                if (failureContainer.hasCriticalFailure(testCaseId)) {
                    echo("Critical failure detected, aborting run phase");
                    return;
                }
                if (failureContainer.hasCriticalFailure() && testSuite.isFailFast()) {
                    echo("Aborting run phase due to failure");
                    return;
                }

                sleepSeconds(logRunPhaseIntervalSeconds);
                logProgress(logRunPhaseIntervalSeconds * i, sleepSeconds);
            }

            if (isRunning) {
                int sleepTime = sleepSeconds % logRunPhaseIntervalSeconds;
                if (sleepTime > 0) {
                    sleepSeconds(sleepSeconds % logRunPhaseIntervalSeconds);
                    logProgress(sleepSeconds, sleepSeconds);
                }
            }
        }

        private void logProgress(int elapsed, int sleepSeconds) {
            String msg = format("Running %s (%s%%)", secondsToHuman(elapsed), formatPercentage(elapsed, sleepSeconds));
            if (monitorPerformance && elapsed % logPerformanceIntervalSeconds == 0) {
                msg += performanceStateContainer.formatPerformanceNumbers(testCaseId);
            }

            LOGGER.info(prefix + msg);
        }
    }
}
