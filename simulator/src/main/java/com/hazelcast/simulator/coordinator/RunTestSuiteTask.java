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

import com.hazelcast.simulator.cluster.ClusterLayout;
import com.hazelcast.simulator.common.SimulatorProperties;
import com.hazelcast.simulator.common.TestCase;
import com.hazelcast.simulator.common.TestSuite;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.registry.ComponentRegistry;
import com.hazelcast.simulator.protocol.registry.TargetType;
import com.hazelcast.simulator.protocol.registry.TestData;
import com.hazelcast.simulator.testcontainer.TestPhase;
import com.hazelcast.simulator.utils.ThreadSpawner;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import static com.hazelcast.simulator.testcontainer.TestPhase.getTestPhaseSyncMap;
import static com.hazelcast.simulator.utils.CommonUtils.getElapsedSeconds;
import static com.hazelcast.simulator.utils.CommonUtils.rethrow;
import static com.hazelcast.simulator.utils.FormatUtils.HORIZONTAL_RULER;
import static com.hazelcast.simulator.utils.FormatUtils.secondsToHuman;
import static java.lang.String.format;

public class RunTestSuiteTask {
    private static final Logger LOGGER = Logger.getLogger(RunTestSuiteTask.class);

    private final TestSuite testSuite;
    private final CoordinatorParameters coordinatorParameters;
    private final ComponentRegistry componentRegistry;
    private final Echoer echoer;
    private final FailureContainer failureContainer;
    private final TestPhaseListeners testPhaseListeners;
    private final SimulatorProperties simulatorProperties;
    private final RemoteClient remoteClient;
    private final ClusterLayout clusterLayout;
    private final PerformanceStatsContainer performanceStatsContainer;
    private final WorkerParameters workerParameters;

    public RunTestSuiteTask(TestSuite testSuite,
                            CoordinatorParameters coordinatorParameters,
                            ComponentRegistry componentRegistry,
                            FailureContainer failureContainer,
                            TestPhaseListeners testPhaseListeners,
                            SimulatorProperties simulatorProperties,
                            RemoteClient remoteClient,
                            ClusterLayout clusterLayout,
                            PerformanceStatsContainer performanceStatsContainer,
                            WorkerParameters workerParameters) {
        this.testSuite = testSuite;
        this.coordinatorParameters = coordinatorParameters;
        this.componentRegistry = componentRegistry;
        this.echoer = new Echoer(remoteClient);
        this.failureContainer = failureContainer;
        this.testPhaseListeners = testPhaseListeners;
        this.simulatorProperties = simulatorProperties;
        this.remoteClient = remoteClient;
        this.clusterLayout = clusterLayout;
        this.performanceStatsContainer = performanceStatsContainer;
        this.workerParameters = workerParameters;
    }

    public void run() {
        try {
            int testCount = testSuite.size();
            boolean parallel = coordinatorParameters.isParallel() && testCount > 1;
            Map<TestPhase, CountDownLatch> testPhaseSyncMap = getTestPhaseSyncMap(testCount, parallel,
                    coordinatorParameters.getLastTestPhaseToSync());

            echoer.echo("Starting TestSuite: %s", testSuite.getId());
            logTestSuiteDuration(parallel);

            for (TestData testData : componentRegistry.getTests()) {
                int testIndex = testData.getTestIndex();
                TestCase testCase = testData.getTestCase();
                echoer.echo("Configuration for %s (T%d):%n%s", testCase.getId(), testIndex, testCase);

                TestCaseRunner runner = new TestCaseRunner(
                        testIndex,
                        testCase,
                        testSuite,
                        remoteClient,
                        testPhaseSyncMap,
                        failureContainer,
                        componentRegistry,
                        coordinatorParameters,
                        workerParameters,
                        performanceStatsContainer);
                testPhaseListeners.addListener(testIndex, runner);
            }

            echoTestSuiteStart(testCount, parallel);
            long started = System.nanoTime();
            if (parallel) {
                runParallel();
            } else {
                runSequential();
            }
            echoTestSuiteEnd(testCount, started);
        } finally {
            int runningWorkerCount = componentRegistry.workerCount();
            echoer.echo("Terminating %d Workers...", runningWorkerCount);
            remoteClient.terminateWorkers(true);

            int waitForWorkerShutdownTimeoutSeconds = simulatorProperties.getWaitForWorkerShutdownTimeoutSeconds();
            if (!failureContainer.waitForWorkerShutdown(runningWorkerCount, waitForWorkerShutdownTimeoutSeconds)) {
                Set<SimulatorAddress> finishedWorkers = failureContainer.getFinishedWorkers();
                LOGGER.warn(format("Unfinished workers: %s", componentRegistry.getMissingWorkers(finishedWorkers).toString()));
            }

            performanceStatsContainer.logDetailedPerformanceInfo(testSuite.getDurationSeconds());
        }
    }

    private void logTestSuiteDuration(boolean isParallel) {
        int testDuration = testSuite.getDurationSeconds();
        if (testDuration > 0) {
            echoer.echo("Running time per test: %s", secondsToHuman(testDuration));
            int totalDuration = (isParallel ? testDuration : testDuration * testSuite.size());
            if (testSuite.isWaitForTestCase()) {
                echoer.echo("Testsuite will run until tests are finished for a maximum time of: %s",
                        secondsToHuman(totalDuration));
            } else {
                echoer.echo("Expected total TestSuite time: %s", secondsToHuman(totalDuration));
            }
        } else if (testSuite.isWaitForTestCase()) {
            echoer.echo("Testsuite will run until tests are finished");
        }
    }

    private void runParallel() {
        ThreadSpawner spawner = new ThreadSpawner("runParallel", true);
        for (final TestPhaseListener testCaseRunner : testPhaseListeners.getListeners()) {
            spawner.spawn(new Runnable() {
                @Override
                public void run() {
                    try {
                        ((TestCaseRunner) testCaseRunner).run();
                    } catch (Exception e) {
                        throw rethrow(e);
                    }
                }
            });
        }
        spawner.awaitCompletion();
    }

    private void runSequential() {
        int testIndex = 0;
        for (TestPhaseListener testCaseRunner : testPhaseListeners.getListeners()) {
            ((TestCaseRunner) testCaseRunner).run();
            boolean hasCriticalFailure = failureContainer.hasCriticalFailure();
            if (hasCriticalFailure && testSuite.isFailFast()) {
                echoer.echo("Aborting TestSuite due to critical failure");
                break;
            }
            // restart Workers if needed, but not after last test
            if ((hasCriticalFailure || coordinatorParameters.isRefreshJvm()) && ++testIndex < testSuite.size()) {
                remoteClient.terminateWorkers(false);
                new StartWorkersTask(
                        clusterLayout,
                        remoteClient,
                        componentRegistry,
                        coordinatorParameters.getWorkerVmStartupDelayMs()).run();
            }
        }
    }

    private void echoTestSuiteStart(int testCount, boolean isParallel) {
        echoer.echo(HORIZONTAL_RULER);
        if (testCount == 1) {
            echoer.echo("Running test...");
        } else {
            echoer.echo("Running %s tests (%s)", testCount, isParallel ? "parallel" : "sequentially");
        }
        echoer.echo(HORIZONTAL_RULER);

        int targetCount = coordinatorParameters.getTargetCount();
        if (targetCount > 0) {
            TargetType targetType = coordinatorParameters.getTargetType(componentRegistry.hasClientWorkers());
            List<String> targetWorkers = componentRegistry.getWorkerAddresses(targetType, targetCount);
            echoer.echo("RUN phase will be executed on %s: %s", targetType.toString(targetCount), targetWorkers);
        }
    }

    private void echoTestSuiteEnd(int testCount, long started) {
        echoer.echo(HORIZONTAL_RULER);
        if (testCount == 1) {
            echoer.echo("Finished running of test (%s)", secondsToHuman(getElapsedSeconds(started)));
        } else {
            echoer.echo("Finished running of %d tests (%s)", testCount, secondsToHuman(getElapsedSeconds(started)));
        }
        echoer.echo(HORIZONTAL_RULER);
    }

}
