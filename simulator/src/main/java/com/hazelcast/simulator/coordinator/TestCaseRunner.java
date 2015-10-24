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
import com.hazelcast.simulator.test.FailureType;
import com.hazelcast.simulator.test.TestCase;
import com.hazelcast.simulator.test.TestPhase;
import com.hazelcast.simulator.test.TestSuite;
import org.apache.log4j.Logger;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

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

    private final TestCase testCase;
    private final String testCaseId;
    private final TestSuite testSuite;
    private final Set<FailureType> nonCriticalFailures;

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

    TestCaseRunner(TestCase testCase, Coordinator coordinator, int paddingLength,
                   ConcurrentMap<TestPhase, CountDownLatch> testPhaseSyncMap) {
        this.testCase = testCase;
        this.testCaseId = testCase.getId();
        this.testSuite = coordinator.getTestSuite();
        this.nonCriticalFailures = testSuite.getTolerableFailures();

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

    boolean run() {
        int oldFailureCount = failureContainer.getFailureCount();
        try {
            createTest();
            runOnAllWorkers(TestPhase.SETUP);

            runOnAllWorkers(TestPhase.LOCAL_WARMUP);
            runOnFirstWorker(TestPhase.GLOBAL_WARMUP);

            startTest();
            waitForTestCompletion();

            if (isVerifyEnabled) {
                runOnFirstWorker(TestPhase.GLOBAL_VERIFY);
                runOnAllWorkers(TestPhase.LOCAL_VERIFY);
            } else {
                echo("Skipping Test verification");
            }

            runOnFirstWorker(TestPhase.GLOBAL_TEARDOWN);
            runOnAllWorkers(TestPhase.LOCAL_TEARDOWN);

            return (failureContainer.getFailureCount() == oldFailureCount);
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    private void createTest() throws TimeoutException {
        echo("Starting Test initialization");
        remoteClient.sendToAllWorkers(new CreateTestOperation(testCase));
        echo("Completed Test initialization");
    }

    private void runOnAllWorkers(TestPhase testPhase) throws TimeoutException {
        echo("Starting Test " + testPhase.desc());
        remoteClient.sendToAllWorkers(new StartTestPhaseOperation(testCaseId, testPhase));
        waitForPhaseCompletion(testPhase, false);
        echo("Completed Test " + testPhase.desc());
        waitForGlobalTestPhaseCompletion(testPhase);
    }

    private void runOnFirstWorker(TestPhase testPhase) throws TimeoutException {
        echo("Starting Test " + testPhase.desc());
        remoteClient.sendToFirstWorker(new StartTestPhaseOperation(testCaseId, testPhase));
        waitForPhaseCompletion(testPhase, true);
        echo("Completed Test " + testPhase.desc());
        waitForGlobalTestPhaseCompletion(testPhase);
    }

    private void startTest() throws TimeoutException {
        echo(format("Starting Test start (%s members)", (isPassiveMembers) ? "passive" : "active"));
        remoteClient.sendToAllWorkers(new StartTestOperation(testCaseId, isPassiveMembers));
        echo("Completed Test start");
    }

    private void waitForTestCompletion() throws TimeoutException, InterruptedException {
        StopThread stopThread = null;
        if (testSuite.getDurationSeconds() > 0) {
            stopThread = new StopThread();
            stopThread.start();
        }

        if (testSuite.isWaitForTestCase()) {
            echo("Test will run until it stops");
            waitForPhaseCompletion(TestPhase.RUN, false);
            echo("Test finished running");

            if (stopThread != null) {
                stopThread.shutdown();
                stopThread.interrupt();
            }
        } else {
            waitForStopThread.await();
        }

        waitForGlobalTestPhaseCompletion(TestPhase.RUN);
    }

    private void waitForPhaseCompletion(TestPhase testPhase, boolean isGlobal) {
        int completedWorkers = phaseCompletedMap.get(testPhase).get();
        int expectedWorkers = getExpectedWorkerCount(isGlobal);

        long started = System.nanoTime();
        while (completedWorkers < expectedWorkers) {
            sleepSeconds(1);

            if (hasCriticalTestFailure()) {
                break;
            }

            completedWorkers = phaseCompletedMap.get(testPhase).get();
            expectedWorkers = getExpectedWorkerCount(isGlobal);

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

    private int getExpectedWorkerCount(boolean isGlobal) {
        int workerCount = componentRegistry.workerCount();
        if (workerCount == 0) {
            return 0;
        }
        return (isGlobal) ? 1 : workerCount;
    }

    private boolean hasCriticalTestFailure() {
        if (failureContainer.hasCriticalFailure(nonCriticalFailures)) {
            echo("Critical failure detected, aborting execution of test");
            return true;
        }
        return false;
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
                remoteClient.sendToAllWorkers(new StopTestOperation(testCaseId));
                waitForPhaseCompletion(TestPhase.RUN, false);
                echo("Completed Test stop");
            } finally {
                waitForStopThread.countDown();
            }
        }

        private void sleepUntilFailure(int sleepSeconds) {
            int sleepLoops = sleepSeconds / logRunPhaseIntervalSeconds;
            for (int i = 1; i <= sleepLoops && isRunning; i++) {
                if (hasCriticalTestFailure()) {
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
            String msg = format("Running %s %s%% complete", secondsToHuman(elapsed), formatPercentage(elapsed, sleepSeconds));
            if (monitorPerformance && elapsed % logPerformanceIntervalSeconds == 0) {
                msg += performanceStateContainer.getPerformanceNumbers(testCaseId);
            }

            LOGGER.info(prefix + msg);
        }
    }
}
