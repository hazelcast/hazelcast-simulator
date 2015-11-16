/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.simulator.test.TestCase;
import com.hazelcast.simulator.test.TestPhase;
import com.hazelcast.simulator.test.TestSuite;
import org.apache.log4j.Logger;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.simulator.test.TestPhase.GLOBAL_TEARDOWN;
import static com.hazelcast.simulator.test.TestPhase.GLOBAL_VERIFY;
import static com.hazelcast.simulator.test.TestPhase.GLOBAL_WARMUP;
import static com.hazelcast.simulator.test.TestPhase.LOCAL_TEARDOWN;
import static com.hazelcast.simulator.test.TestPhase.LOCAL_VERIFY;
import static com.hazelcast.simulator.test.TestPhase.LOCAL_WARMUP;
import static com.hazelcast.simulator.test.TestPhase.RUN;
import static com.hazelcast.simulator.test.TestPhase.SETUP;
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
    private final ConcurrentMap<TestPhase, CountDownLatch> testPhaseSyncMap;

    private final boolean isVerifyEnabled;
    private final boolean isPassiveMembers;

    private final boolean monitorPerformance;
    private final int logPerformanceIntervalSeconds;
    private final int logRunPhaseIntervalSeconds;

    TestCaseRunner(int testIndex, TestCase testCase, Coordinator coordinator, int paddingLength,
                   ConcurrentMap<TestPhase, CountDownLatch> testPhaseSyncMap) {
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

        ClusterLayoutParameters clusterLayoutParameters = coordinator.getClusterLayoutParameters();
        this.isPassiveMembers = (coordinatorParameters.isPassiveMembers() && clusterLayoutParameters.getClientWorkerCount() > 0);

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
            runPhase(SETUP);

            runPhase(LOCAL_WARMUP);
            runPhase(GLOBAL_WARMUP);

            startTest();
            waitForTestCompletion();

            if (isVerifyEnabled) {
                runPhase(GLOBAL_VERIFY);
                runPhase(LOCAL_VERIFY);
            } else {
                echo("Skipping Test verification");
            }

            runPhase(GLOBAL_TEARDOWN);
            runPhase(LOCAL_TEARDOWN);
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    private void createTest() throws TimeoutException {
        echo("Starting Test initialization");
        remoteClient.sendToAllWorkers(new CreateTestOperation(testIndex, testCase));
        echo("Completed Test initialization");
    }

    private void runPhase(TestPhase testPhase) throws TimeoutException {
        if (testSuite.isFailFast() && failureContainer.hasCriticalFailure(testCaseId)) {
            echo("Skipping Test " + testPhase.desc() + " (critical failure)");
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

    private void startTest() throws TimeoutException {
        echo(format("Starting Test start (%s members)", (isPassiveMembers) ? "passive" : "active"));
        remoteClient.sendToTestOnAllWorkers(testCaseId, new StartTestOperation(isPassiveMembers));
        echo("Completed Test start");
    }

    private void waitForTestCompletion() throws Exception {
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
        try {
            CountDownLatch latch = testPhaseSyncMap.get(testPhase);
            latch.countDown();
            latch.await();
            if (LOG_TEST_PHASE_COMPLETION.putIfAbsent(testPhase, true) == null) {
                LOGGER.info("Completed TestPhase " + testPhase.desc());
            }
        } catch (InterruptedException e) {
            LOGGER.warn("Interrupted Test while waiting for " + testPhase.desc() + " completion", e);
        }
    }

    private int getExpectedWorkerCount(TestPhase testPhase) {
        int workerCount = componentRegistry.workerCount();
        if (workerCount == 0) {
            return 0;
        }
        return (testPhase.isGlobal()) ? 1 : workerCount;
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
                    echo("Critical failure detected, aborting execution of test");
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
                msg += performanceStateContainer.getPerformanceNumbers(testCaseId);
            }

            LOGGER.info(prefix + msg);
        }
    }
}
