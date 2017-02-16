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

package com.hazelcast.simulator.coordinator.tasks;

import com.hazelcast.simulator.common.TestCase;
import com.hazelcast.simulator.common.TestPhase;
import com.hazelcast.simulator.coordinator.CoordinatorParameters;
import com.hazelcast.simulator.coordinator.FailureCollector;
import com.hazelcast.simulator.coordinator.PerformanceStatsCollector;
import com.hazelcast.simulator.coordinator.RemoteClient;
import com.hazelcast.simulator.coordinator.TestCaseRunner;
import com.hazelcast.simulator.coordinator.TestPhaseListeners;
import com.hazelcast.simulator.coordinator.TestSuite;
import com.hazelcast.simulator.protocol.registry.ComponentRegistry;
import com.hazelcast.simulator.protocol.registry.TestData;
import com.hazelcast.simulator.protocol.registry.WorkerData;
import com.hazelcast.simulator.protocol.registry.WorkerQuery;
import com.hazelcast.simulator.utils.ThreadSpawner;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.simulator.protocol.registry.WorkerData.toAddressString;
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
    private final FailureCollector failureCollector;
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
        this.failureCollector = failureCollector;
        this.testPhaseListeners = testPhaseListeners;
        this.remoteClient = remoteClient;
        this.performanceStatsCollector = performanceStatsCollector;
    }

    public boolean run() {
        List<TestData> tests = componentRegistry.addTests(testSuite);
        try {
            List<WorkerData> targets = initTargets();
            return run0(tests, targets);
        } finally {
            testPhaseListeners.removeAllListeners(runners);
        }
    }

    private List<WorkerData> initTargets() {
        WorkerQuery workerQuery = testSuite.getWorkerQuery();
        List<WorkerData> targets = workerQuery.execute(componentRegistry.getWorkers());
        if (targets.isEmpty()) {
            throw new IllegalStateException("No workers found for query: " + workerQuery);
        }

        List<WorkerData> clients = filter(targets, false);
        List<WorkerData> members = filter(targets, true);

        LOGGER.info("Using: " + workerQuery);
        if (members.isEmpty()) {
            LOGGER.info(format("Using %s clients [%s]", clients.size(), toAddressString(clients)));
        } else if (clients.isEmpty()) {
            LOGGER.info(format("Using %s members [%s]", members.size(), toAddressString(members)));
        } else {
            LOGGER.info(format("Using %s clients [%s]", clients.size(), toAddressString(clients)));
            LOGGER.info(format("Using %s members [%s]", members.size(), toAddressString(members)));
        }
        return targets;
    }

    private List<WorkerData> filter(List<WorkerData> targets, boolean isMember) {
        List<WorkerData> result = new ArrayList<WorkerData>(targets.size());
        for (WorkerData worker : targets) {
            if (worker.isMemberWorker() == isMember) {
                result.add(worker);
            }
        }
        return result;
    }

    private boolean run0(List<TestData> tests, List<WorkerData> targets) {
        int testCount = testSuite.size();
        boolean parallel = testSuite.isParallel() && testCount > 1;
        Map<TestPhase, CountDownLatch> testPhaseSyncMap = getTestPhaseSyncMap(testCount, parallel,
                coordinatorParameters.getLastTestPhaseToSync());

        LOGGER.info("Starting TestSuite");
        echoTestSuiteDuration(parallel);

        for (TestData testData : tests) {
            int testIndex = testData.getTestIndex();
            TestCase testCase = testData.getTestCase();
            LOGGER.info(format("Configuration for %s (T%d):%n%s", testCase.getId(), testIndex, testCase));

            TestCaseRunner runner = new TestCaseRunner(
                    testData,
                    targets,
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
        boolean success = parallel ? runParallel() : runSequential();
        echoTestSuiteEnd(testCount, started);
        return success;
    }

    private void echoTestSuiteDuration(boolean isParallel) {
        int testDuration = testSuite.getDurationSeconds();
        if (testDuration > 0) {
            LOGGER.info(format("Running time per test: %s", secondsToHuman(testDuration)));
            int totalDuration = isParallel ? testDuration : testDuration * testSuite.size();
            LOGGER.info(format("Testsuite will run until tests are finished for a maximum time of: %s",
                    secondsToHuman(totalDuration)));
        } else {
            LOGGER.info("Testsuite will run until tests are finished");
        }
    }

    private boolean runParallel() {
        final AtomicBoolean success = new AtomicBoolean(true);
        ThreadSpawner spawner = new ThreadSpawner("runParallel", true);
        for (final TestCaseRunner runner : runners) {
            spawner.spawn(new Runnable() {
                @Override
                public void run() {
                    try {
                        if (!runner.run()) {
                            success.set(false);
                        }
                    } catch (Exception e) {
                        throw rethrow(e);
                    }
                }
            });
        }
        spawner.awaitCompletion();
        return success.get();
    }

    private boolean runSequential() {
        boolean success = true;
        for (TestCaseRunner runner : runners) {
            if (!runner.run()) {
                success = false;
            }
            boolean hasCriticalFailure = failureCollector.hasCriticalFailure();
            if (hasCriticalFailure && testSuite.isFailFast()) {
                LOGGER.info("Aborting TestSuite due to critical failure");
                break;
            }
        }
        return success;
    }

    private void echoTestSuiteStart(int testCount, boolean isParallel) {
        LOGGER.info(HORIZONTAL_RULER);
        if (testCount == 1) {
            LOGGER.info("Running test...");
        } else {
            LOGGER.info(format("Running %s tests (%s)", testCount, isParallel ? "parallel" : "sequentially"));
        }
        LOGGER.info(HORIZONTAL_RULER);
    }

    private void echoTestSuiteEnd(int testCount, long started) {
        LOGGER.info(HORIZONTAL_RULER);
        if (testCount == 1) {
            LOGGER.info(format("Finished running of test (%s)", secondsToHuman(getElapsedSeconds(started))));
        } else {
            LOGGER.info(format("Finished running of %d tests (%s)", testCount, secondsToHuman(getElapsedSeconds(started))));
        }
        LOGGER.info(HORIZONTAL_RULER);
    }

    static Map<TestPhase, CountDownLatch> getTestPhaseSyncMap(int testCount, boolean parallel, TestPhase latestTestPhaseToSync) {
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
