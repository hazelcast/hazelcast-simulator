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
import com.hazelcast.simulator.common.TestSuite;
import com.hazelcast.simulator.protocol.registry.ComponentRegistry;
import com.hazelcast.simulator.protocol.registry.TargetType;
import com.hazelcast.simulator.protocol.registry.TestData;
import com.hazelcast.simulator.utils.ThreadSpawner;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import static com.hazelcast.simulator.utils.CommonUtils.getElapsedSeconds;
import static com.hazelcast.simulator.utils.CommonUtils.rethrow;
import static com.hazelcast.simulator.utils.FormatUtils.HORIZONTAL_RULER;
import static com.hazelcast.simulator.utils.FormatUtils.secondsToHuman;

public class RunTestSuiteTask {

    private final TestSuite testSuite;
    private final CoordinatorParameters coordinatorParameters;
    private final ComponentRegistry componentRegistry;
    private final Echoer echoer;
    private final FailureCollector failureCollector;
    //todo: should this argument be passed or is it part of the RunTestSuiteTask?
    private final TestPhaseListeners testPhaseListeners;
    private final RemoteClient remoteClient;
    private final PerformanceStatsCollector performanceStatsCollector;
    private final List<TestCaseRunner> runners = new ArrayList<TestCaseRunner>();

    public RunTestSuiteTask(TestSuite testSuite,
                            CoordinatorParameters coordinatorParameters,
                            ComponentRegistry componentRegistry,
                            FailureCollector failureCollector,
                            TestPhaseListeners testPhaseListeners,
                            RemoteClient remoteClient,
                            PerformanceStatsCollector performanceStatsCollector) {
        this.testSuite = testSuite;
        this.coordinatorParameters = coordinatorParameters;
        this.componentRegistry = componentRegistry;
        this.echoer = new Echoer(remoteClient);
        this.failureCollector = failureCollector;
        this.testPhaseListeners = testPhaseListeners;
        this.remoteClient = remoteClient;
        this.performanceStatsCollector = performanceStatsCollector;
    }

    public void run() {
        List<TestData> tests = componentRegistry.addTests(testSuite);
        try {
            run0(tests);
        } finally {
            testPhaseListeners.removeAllListeners(runners);
            componentRegistry.removeTests(testSuite);
            performanceStatsCollector.logDetailedPerformanceInfo(testSuite.getDurationSeconds());
        }
    }

    private void run0(List<TestData> tests) {
        int testCount = testSuite.size();
        boolean parallel = testSuite.isParallel() && testCount > 1;
        Map<TestPhase, CountDownLatch> testPhaseSyncMap = getTestPhaseSyncMap(testCount, parallel,
                coordinatorParameters.getLastTestPhaseToSync());

        echoer.echo("Starting TestSuite");
        echoTestSuiteDuration(parallel);

        for (TestData testData: tests) {
            int testIndex = testData.getTestIndex();
            TestCase testCase = testData.getTestCase();
            echoer.echo("Configuration for %s (T%d):%n%s", testCase.getId(), testIndex, testCase);

            TestCaseRunner runner = new TestCaseRunner(
                    testIndex,
                    testCase,
                    testSuite,
                    remoteClient,
                    testPhaseSyncMap,
                    failureCollector,
                    componentRegistry,
                    performanceStatsCollector,
                    coordinatorParameters.getPerformanceMonitorIntervalSeconds());
            runners.add(runner);
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
    }

    private void echoTestSuiteDuration(boolean isParallel) {
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
        for (final TestCaseRunner runner : runners) {
            spawner.spawn(new Runnable() {
                @Override
                public void run() {
                    try {
                        runner.run();
                    } catch (Exception e) {
                        throw rethrow(e);
                    }
                }
            });
        }
        spawner.awaitCompletion();
    }

    private void runSequential() {
        for (TestCaseRunner runner : runners) {
            runner.run();
            boolean hasCriticalFailure = failureCollector.hasCriticalFailure();
            if (hasCriticalFailure && testSuite.isFailFast()) {
                echoer.echo("Aborting TestSuite due to critical failure");
                break;
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

        int targetCount = testSuite.getTargetCount();
        if (targetCount > 0) {
            TargetType targetType = testSuite.getTargetType().resolvePreferClient(componentRegistry.hasClientWorkers());
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

    static Map<TestPhase, CountDownLatch> getTestPhaseSyncMap(int testCount, boolean parallel,
                                                                     TestPhase latestTestPhaseToSync) {
        if (!parallel) {
            return null;
        }
        Map<TestPhase, CountDownLatch> testPhaseSyncMap = new ConcurrentHashMap<TestPhase, CountDownLatch>();
        boolean setTestCount = true;
        for (TestPhase testPhase : TestPhase.values()) {
            testPhaseSyncMap.put(testPhase, new CountDownLatch(setTestCount ? testCount : 0));
            if (testPhase.equals(latestTestPhaseToSync)) {
                setTestCount = false;
            }
        }
        return testPhaseSyncMap;
    }
}
