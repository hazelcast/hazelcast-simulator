package com.hazelcast.simulator.coordinator;

import com.hazelcast.simulator.protocol.operation.CreateTestOperation;
import com.hazelcast.simulator.protocol.operation.StartTestOperation;
import com.hazelcast.simulator.protocol.operation.StartTestPhaseOperation;
import com.hazelcast.simulator.protocol.operation.StopTestOperation;
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

import static com.hazelcast.simulator.coordinator.Coordinator.HORIZONTAL_RULER;
import static com.hazelcast.simulator.utils.CommonUtils.sleepSeconds;
import static com.hazelcast.simulator.utils.FormatUtils.formatPercentage;
import static com.hazelcast.simulator.utils.FormatUtils.padRight;
import static com.hazelcast.simulator.utils.FormatUtils.secondsToHuman;
import static java.lang.String.format;

/**
 * TestCaseRunner is responsible for running a single TestCase. Multiple TestCases can be run in parallel,
 * by having multiple TestCaseRunners in parallel.
 */
final class TestCaseRunner {

    private static final Logger LOGGER = Logger.getLogger(TestCaseRunner.class);
    private static final ConcurrentMap<TestPhase, Object> LOG_TEST_PHASE_COMPLETION = new ConcurrentHashMap<TestPhase, Object>();

    private final CountDownLatch waitForStopThread = new CountDownLatch(1);

    private final TestCase testCase;
    private final TestSuite testSuite;
    private final CoordinatorParameters coordinatorParameters;
    private final RemoteClient remoteClient;
    private final FailureContainer failureContainer;
    private final PerformanceStateContainer performanceStateContainer;
    private final Set<FailureType> nonCriticalFailures;
    private final String testCaseId;
    private final String prefix;
    private final ConcurrentMap<TestPhase, CountDownLatch> testPhaseSyncMap;

    private final int sleepPeriodSeconds;
    private final boolean monitorPerformance;
    private final int logPerformanceInterval;

    private final int clientWorkerCount;

    private StopThread stopThread;

    TestCaseRunner(TestCase testCase, TestSuite testSuite, Coordinator coordinator, RemoteClient remoteClient,
                   FailureContainer failureContainer, PerformanceStateContainer performanceStateContainer, int paddingLength,
                   ConcurrentMap<TestPhase, CountDownLatch> testPhaseSyncMap, int sleepPeriodSeconds) {
        this.testCase = testCase;
        this.testSuite = testSuite;
        this.coordinatorParameters = coordinator.getCoordinatorParameters();
        this.remoteClient = remoteClient;
        this.failureContainer = failureContainer;
        this.performanceStateContainer = performanceStateContainer;
        this.nonCriticalFailures = testSuite.getTolerableFailures();
        this.testCaseId = testCase.getId();
        this.prefix = (testCaseId.isEmpty() ? "" : padRight(testCaseId, paddingLength + 1));
        this.testPhaseSyncMap = testPhaseSyncMap;
        this.sleepPeriodSeconds = sleepPeriodSeconds;

        WorkerParameters workerParameters = coordinator.getWorkerParameters();
        this.monitorPerformance = workerParameters.isMonitorPerformance();
        this.logPerformanceInterval = workerParameters.getWorkerPerformanceMonitorIntervalSeconds();

        this.clientWorkerCount = coordinator.getClusterLayoutParameters().getClientWorkerCount();
    }

    boolean run() {
        LOGGER.info(format("%s%nRunning Test: %s%n%s%n%s", HORIZONTAL_RULER, testCaseId, testCase, HORIZONTAL_RULER));

        int oldFailureCount = failureContainer.getFailureCount();
        try {
            initTestCase();
            runOnAllWorkers(TestPhase.SETUP);

            runOnAllWorkers(TestPhase.LOCAL_WARMUP);
            runOnFirstWorker(TestPhase.GLOBAL_WARMUP);

            startTestCase();
            waitForTestCase();

            if (coordinatorParameters.isVerifyEnabled()) {
                runOnFirstWorker(TestPhase.GLOBAL_VERIFY);
                runOnAllWorkers(TestPhase.LOCAL_VERIFY);
            } else {
                echo("Skipping Test verification");
            }

            runOnFirstWorker(TestPhase.GLOBAL_TEARDOWN);
            runOnAllWorkers(TestPhase.LOCAL_TEARDOWN);

            return (failureContainer.getFailureCount() == oldFailureCount);
        } catch (Exception e) {
            LOGGER.error(format("%s in TestCaseRunner: %s", e.getClass().getSimpleName(), e.getMessage()));
            return false;
        }
    }

    private void initTestCase() throws TimeoutException {
        echo("Starting Test initialization");
        remoteClient.sendToAllWorkers(new CreateTestOperation(testCase));
        echo("Completed Test initialization");
    }

    private void runOnAllWorkers(TestPhase testPhase) throws TimeoutException {
        echo("Starting Test " + testPhase.desc());
        remoteClient.sendToAllWorkers(new StartTestPhaseOperation(testCaseId, testPhase));
        remoteClient.waitForPhaseCompletion(prefix, testCaseId, testPhase);
        echo("Completed Test " + testPhase.desc());
        waitForTestPhaseCompletion(testPhase);
    }

    private void runOnFirstWorker(TestPhase testPhase) throws TimeoutException {
        echo("Starting Test " + testPhase.desc());
        remoteClient.sendToFirstWorker(new StartTestPhaseOperation(testCaseId, testPhase));
        remoteClient.waitForPhaseCompletion(prefix, testCaseId, testPhase);
        echo("Completed Test " + testPhase.desc());
        waitForTestPhaseCompletion(testPhase);
    }

    private void waitForTestPhaseCompletion(TestPhase testPhase) {
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

    private void startTestCase() throws TimeoutException {
        boolean isPassiveMembers = (coordinatorParameters.isPassiveMembers() && clientWorkerCount > 0);

        echo(format("Starting Test start (%s members)", (isPassiveMembers) ? "passive" : "active"));
        remoteClient.sendToAllWorkers(new StartTestOperation(testCaseId, isPassiveMembers));
        echo("Completed Test start");
    }

    private void waitForTestCase() throws TimeoutException, InterruptedException {
        if (testSuite.getDurationSeconds() > 0) {
            stopThread = new StopThread();
            stopThread.start();
        }

        if (testSuite.isWaitForTestCase()) {
            echo("Test will run until it stops");
            remoteClient.waitForPhaseCompletion(prefix, testCaseId, TestPhase.RUN);
            echo("Test finished running");

            if (stopThread != null) {
                stopThread.shutdown();
                stopThread.interrupt();
            }
        } else {
            waitForStopThread.await();
        }

        waitForTestPhaseCompletion(TestPhase.RUN);
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
                remoteClient.waitForPhaseCompletion(prefix, testCaseId, TestPhase.RUN);
                echo("Completed Test stop");
            } finally {
                waitForStopThread.countDown();
            }
        }

        private void sleepUntilFailure(int sleepSeconds) {
            int sleepLoops = sleepSeconds / sleepPeriodSeconds;
            for (int i = 1; i <= sleepLoops; i++) {
                if (failureContainer.hasCriticalFailure(nonCriticalFailures)) {
                    echo("Critical Failure detected, aborting execution of test");
                    return;
                }

                sleepSeconds(sleepPeriodSeconds);

                logProgress(sleepPeriodSeconds * i, sleepSeconds);

                if (!isRunning) {
                    break;
                }
            }

            if (isRunning) {
                int sleepTime = sleepSeconds % sleepPeriodSeconds;
                if (sleepTime > 0) {
                    sleepSeconds(sleepSeconds % sleepPeriodSeconds);
                    logProgress(sleepSeconds, sleepSeconds);
                }
            }
        }

        private void logProgress(int elapsed, int sleepSeconds) {
            String msg = format("Running %s %s%% complete", secondsToHuman(elapsed), formatPercentage(elapsed, sleepSeconds));
            if (monitorPerformance && elapsed % logPerformanceInterval == 0) {
                msg += performanceStateContainer.getPerformanceNumbers(testCaseId);
            }

            LOGGER.info(prefix + msg);
        }
    }
}
