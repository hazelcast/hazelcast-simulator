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
import com.hazelcast.simulator.protocol.connector.CoordinatorConnector;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.operation.OperationTypeCounter;
import com.hazelcast.simulator.protocol.registry.AgentData;
import com.hazelcast.simulator.protocol.registry.ComponentRegistry;
import com.hazelcast.simulator.protocol.registry.TargetType;
import com.hazelcast.simulator.protocol.registry.TestData;
import com.hazelcast.simulator.protocol.registry.WorkerData;
import com.hazelcast.simulator.test.TestCase;
import com.hazelcast.simulator.test.TestPhase;
import com.hazelcast.simulator.test.TestSuite;
import com.hazelcast.simulator.utils.Bash;
import com.hazelcast.simulator.utils.CommandLineExitException;
import com.hazelcast.simulator.utils.ThreadSpawner;
import com.hazelcast.simulator.utils.jars.HazelcastJARs;
import org.apache.log4j.Logger;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.simulator.agent.workerjvm.WorkerJvmLauncher.WORKERS_HOME_NAME;
import static com.hazelcast.simulator.coordinator.CoordinatorCli.init;
import static com.hazelcast.simulator.test.TestPhase.getTestPhaseSyncMap;
import static com.hazelcast.simulator.utils.AgentUtils.startAgents;
import static com.hazelcast.simulator.utils.AgentUtils.stopAgents;
import static com.hazelcast.simulator.utils.CloudProviderUtils.isLocal;
import static com.hazelcast.simulator.utils.CommonUtils.exitWithError;
import static com.hazelcast.simulator.utils.CommonUtils.getElapsedSeconds;
import static com.hazelcast.simulator.utils.CommonUtils.getSimulatorVersion;
import static com.hazelcast.simulator.utils.CommonUtils.rethrow;
import static com.hazelcast.simulator.utils.CommonUtils.sleepSeconds;
import static com.hazelcast.simulator.utils.FileUtils.ensureExistingDirectory;
import static com.hazelcast.simulator.utils.FileUtils.getSimulatorHome;
import static com.hazelcast.simulator.utils.FormatUtils.HORIZONTAL_RULER;
import static com.hazelcast.simulator.utils.FormatUtils.secondsToHuman;
import static com.hazelcast.simulator.utils.NativeUtils.execute;
import static java.lang.String.format;

public final class Coordinator {

    static final String SIMULATOR_VERSION = getSimulatorVersion();

    private static final int WAIT_FOR_WORKER_FAILURE_RETRY_COUNT = 10;

    private static final Logger LOGGER = Logger.getLogger(Coordinator.class);

    private final TestPhaseListenerContainer testPhaseListenerContainer = new TestPhaseListenerContainer();
    private final PerformanceStateContainer performanceStateContainer = new PerformanceStateContainer();
    private final TestHistogramContainer testHistogramContainer = new TestHistogramContainer(performanceStateContainer);

    private final TestSuite testSuite;
    private final ComponentRegistry componentRegistry;
    private final CoordinatorParameters coordinatorParameters;
    private final WorkerParameters workerParameters;
    private final ClusterLayoutParameters clusterLayoutParameters;

    private final FailureContainer failureContainer;

    private final SimulatorProperties simulatorProperties;
    private final Bash bash;

    private final ClusterLayout clusterLayout;
    private final HazelcastJARs hazelcastJARs;
    private final TestPhase lastTestPhaseToSync;

    private RemoteClient remoteClient;
    private CoordinatorConnector coordinatorConnector;

    public Coordinator(TestSuite testSuite, ComponentRegistry componentRegistry, CoordinatorParameters coordinatorParameters,
                       WorkerParameters workerParameters, ClusterLayoutParameters clusterLayoutParameters) {
        this.testSuite = testSuite;
        this.componentRegistry = componentRegistry;
        this.coordinatorParameters = coordinatorParameters;
        this.workerParameters = workerParameters;
        this.clusterLayoutParameters = clusterLayoutParameters;

        this.failureContainer = new FailureContainer(testSuite, componentRegistry);

        this.simulatorProperties = coordinatorParameters.getSimulatorProperties();
        this.bash = new Bash(simulatorProperties);

        this.clusterLayout = new ClusterLayout(componentRegistry, workerParameters, clusterLayoutParameters);
        this.hazelcastJARs = HazelcastJARs.newInstance(bash, simulatorProperties, clusterLayout.getVersionSpecs());
        this.lastTestPhaseToSync = coordinatorParameters.getLastTestPhaseToSync();

        logConfiguration();
    }

    CoordinatorParameters getCoordinatorParameters() {
        return coordinatorParameters;
    }

    WorkerParameters getWorkerParameters() {
        return workerParameters;
    }

    ClusterLayoutParameters getClusterLayoutParameters() {
        return clusterLayoutParameters;
    }

    TestSuite getTestSuite() {
        return testSuite;
    }

    ComponentRegistry getComponentRegistry() {
        return componentRegistry;
    }

    FailureContainer getFailureContainer() {
        return failureContainer;
    }

    PerformanceStateContainer getPerformanceStateContainer() {
        return performanceStateContainer;
    }

    RemoteClient getRemoteClient() {
        return remoteClient;
    }

    // just for testing
    void setRemoteClient(RemoteClient remoteClient) {
        this.remoteClient = remoteClient;
    }

    // just for testing
    TestPhaseListenerContainer getTestPhaseListenerContainer() {
        return testPhaseListenerContainer;
    }

    private void logConfiguration() {
        echoLocal("Total number of agents: %s", componentRegistry.agentCount());
        echoLocal("Total number of Hazelcast member workers: %s", clusterLayout.getMemberWorkerCount());
        echoLocal("Total number of Hazelcast client workers: %s", clusterLayout.getClientWorkerCount());
        echoLocal("Last TestPhase to sync: %s", lastTestPhaseToSync);

        boolean performanceEnabled = workerParameters.isMonitorPerformance();
        int performanceIntervalSeconds = workerParameters.getWorkerPerformanceMonitorIntervalSeconds();
        echoLocal("Performance monitor enabled: %s (%d seconds)", performanceEnabled, performanceIntervalSeconds);
    }

    private void run() {
        try {
            uploadFiles();

            startAgents(LOGGER, bash, simulatorProperties, componentRegistry);
            startCoordinatorConnector();
            startRemoteClient();
            startWorkers();

            runTestSuite();
        } catch (CommandLineExitException e) {
            for (int i = 0; i < WAIT_FOR_WORKER_FAILURE_RETRY_COUNT && failureContainer.getFailureCount() == 0; i++) {
                sleepSeconds(1);
            }
            throw e;
        } finally {
            try {
                failureContainer.logFailureInfo();
            } finally {
                if (coordinatorConnector != null) {
                    echo("Shutdown of ClientConnector...");
                    coordinatorConnector.shutdown();
                }
                if (hazelcastJARs != null) {
                    hazelcastJARs.shutdown();
                }
                stopAgents(LOGGER, bash, simulatorProperties, componentRegistry);
                moveLogFiles();
                OperationTypeCounter.printStatistics();
            }
        }
    }

    private void uploadFiles() {
        if (isLocal(simulatorProperties)) {
            return;
        }
        CoordinatorUploader uploader = new CoordinatorUploader(bash, componentRegistry, clusterLayout, hazelcastJARs,
                coordinatorParameters.isUploadHazelcastJARs(), coordinatorParameters.isEnterpriseEnabled(),
                coordinatorParameters.getWorkerClassPath(), workerParameters.getProfiler(), testSuite.getId());
        uploader.run();
    }

    private void startCoordinatorConnector() {
        try {
            coordinatorConnector = new CoordinatorConnector(failureContainer, testPhaseListenerContainer,
                    performanceStateContainer, testHistogramContainer);
            ThreadSpawner spawner = new ThreadSpawner("startCoordinatorConnector", true);
            for (final AgentData agentData : componentRegistry.getAgents()) {
                final int agentPort = simulatorProperties.getAgentPort();
                spawner.spawn(new Runnable() {
                    @Override
                    public void run() {
                        coordinatorConnector.addAgent(agentData.getAddressIndex(), agentData.getPublicAddress(), agentPort);
                    }
                });
            }
            spawner.awaitCompletion();
        } catch (Exception e) {
            throw new CommandLineExitException("Could not start CoordinatorConnector", e);
        }
    }

    private void startRemoteClient() {
        int workerPingIntervalMillis = (int) TimeUnit.SECONDS.toMillis(simulatorProperties.getWorkerPingIntervalSeconds());
        int shutdownDelaySeconds = simulatorProperties.getMemberWorkerShutdownDelaySeconds();

        remoteClient = new RemoteClient(coordinatorConnector, componentRegistry, workerPingIntervalMillis, shutdownDelaySeconds);
        remoteClient.initTestSuite(testSuite);
    }

    private void startWorkers() {
        try {
            long started = System.nanoTime();

            echo(HORIZONTAL_RULER);
            echo("Starting Workers...");
            echo(HORIZONTAL_RULER);

            int totalWorkerCount = clusterLayout.getTotalWorkerCount();
            echo("Starting %d Workers (%d members, %d clients)...", totalWorkerCount, clusterLayout.getMemberWorkerCount(),
                    clusterLayout.getClientWorkerCount());
            remoteClient.createWorkers(clusterLayout, true);

            if (componentRegistry.workerCount() > 0) {
                WorkerData firstWorker = componentRegistry.getFirstWorker();
                echo("Worker for global test phases will be %s (%s)", firstWorker.getAddress(),
                        firstWorker.getSettings().getWorkerType());
            }

            long elapsed = getElapsedSeconds(started);
            echo(HORIZONTAL_RULER);
            echo("Finished starting of %s Worker JVMs (%s seconds)", totalWorkerCount, elapsed);
            echo(HORIZONTAL_RULER);
        } catch (Exception e) {
            throw new CommandLineExitException("Failed to start Workers", e);
        }
    }

    void runTestSuite() {
        try {
            int testCount = testSuite.size();
            boolean isParallel = (coordinatorParameters.isParallel() && testCount > 1);
            int maxTestCaseIdLength = testSuite.getMaxTestCaseIdLength();
            Map<TestPhase, CountDownLatch> testPhaseSyncMap = getTestPhaseSyncMap(testCount, isParallel, lastTestPhaseToSync);

            echo("Starting testsuite: %s", testSuite.getId());
            logTestSuiteDuration(isParallel);

            for (TestData testData : componentRegistry.getTests()) {
                int testIndex = testData.getTestIndex();
                TestCase testCase = testData.getTestCase();
                echo("Configuration for %s (T%d):%n%s", testCase.getId(), testIndex, testCase);
                TestCaseRunner runner = new TestCaseRunner(testIndex, testCase, this, maxTestCaseIdLength, testPhaseSyncMap);
                testPhaseListenerContainer.addListener(testIndex, runner);
            }

            echoTestSuiteStart(testCount, isParallel);
            long started = System.nanoTime();
            if (isParallel) {
                runParallel();
            } else {
                runSequential(testCount);
            }
            echoTestSuiteEnd(testCount, started);
        } finally {
            int runningWorkerCount = componentRegistry.workerCount();
            echo("Terminating %d Workers...", runningWorkerCount);
            remoteClient.terminateWorkers(true);

            int waitForWorkerShutdownTimeoutSeconds = simulatorProperties.getWaitForWorkerShutdownTimeoutSeconds();
            if (!failureContainer.waitForWorkerShutdown(runningWorkerCount, waitForWorkerShutdownTimeoutSeconds)) {
                Set<SimulatorAddress> finishedWorkers = failureContainer.getFinishedWorkers();
                LOGGER.warn(format("Unfinished workers: %s", componentRegistry.getMissingWorkers(finishedWorkers).toString()));
            }

            performanceStateContainer.logDetailedPerformanceInfo();
            for (TestCase testCase : testSuite.getTestCaseList()) {
                testHistogramContainer.createProbeResults(testSuite.getId(), testCase.getId());
            }
        }
    }

    private void logTestSuiteDuration(boolean isParallel) {
        int testDuration = testSuite.getDurationSeconds();
        if (testDuration > 0) {
            echo("Running time per test: %s", secondsToHuman(testDuration));
            int totalDuration = (isParallel ? testDuration : testDuration * testSuite.size());
            if (testSuite.isWaitForTestCase()) {
                echo("Testsuite will run until tests are finished for a maximum time of: %s", secondsToHuman(totalDuration));
            } else {
                echo("Expected total testsuite time: %s", secondsToHuman(totalDuration));
            }
        } else if (testSuite.isWaitForTestCase()) {
            echo("Testsuite will run until tests are finished");
        }
    }

    private void runParallel() {
        ThreadSpawner spawner = new ThreadSpawner("runParallel", true);
        for (final TestPhaseListener testCaseRunner : testPhaseListenerContainer.getListeners()) {
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

    private void runSequential(int testCount) {
        int testIndex = 0;
        for (TestPhaseListener testCaseRunner : testPhaseListenerContainer.getListeners()) {
            ((TestCaseRunner) testCaseRunner).run();
            boolean hasCriticalFailure = failureContainer.hasCriticalFailure();
            if (hasCriticalFailure && testSuite.isFailFast()) {
                echo("Aborting testsuite due to critical failure");
                break;
            }
            // restart Workers if needed, but not after last test
            if ((hasCriticalFailure || coordinatorParameters.isRefreshJvm()) && ++testIndex < testCount) {
                startWorkers();
            }
        }
    }

    private void moveLogFiles() {
        if (isLocal(simulatorProperties)) {
            File targetDirectory = ensureExistingDirectory(new File("."), WORKERS_HOME_NAME);

            String targetPath = targetDirectory.getAbsolutePath();
            execute(format("mv %s/%s/* %s || true", getSimulatorHome(), WORKERS_HOME_NAME, targetPath));
            execute(format("mv ./agent.err %s/%s/ || true", targetPath, testSuite.getId()));
            execute(format("mv ./agent.out %s/%s/ || true", targetPath, testSuite.getId()));
        }
    }

    private void echoTestSuiteStart(int testCount, boolean isParallel) {
        echo(HORIZONTAL_RULER);
        if (testCount == 1) {
            echo("Running test...");
        } else {
            echo("Running %s tests (%s)", testCount, isParallel ? "parallel" : "sequentially");
        }
        echo(HORIZONTAL_RULER);

        int targetCount = coordinatorParameters.getTargetCount();
        if (targetCount > 0) {
            TargetType targetType = coordinatorParameters.getTargetType(componentRegistry.hasClientWorkers());
            List<String> targetWorkers = componentRegistry.getWorkerAddresses(targetType, targetCount);
            echo("RUN phase will be executed on %s: %s", targetType.toString(targetCount), targetWorkers);
        }
    }

    private void echoTestSuiteEnd(int testCount, long started) {
        echo(HORIZONTAL_RULER);
        if (testCount == 1) {
            echo("Finished running of test (%s)", secondsToHuman(getElapsedSeconds(started)));
        } else {
            echo("Finished running of %d tests (%s)", testCount, secondsToHuman(getElapsedSeconds(started)));
        }
        echo(HORIZONTAL_RULER);
    }

    private void echoLocal(String msg, Object... args) {
        LOGGER.info(format(msg, args));
    }

    private void echo(String msg, Object... args) {
        String message = format(msg, args);
        remoteClient.logOnAllAgents(message);
        LOGGER.info(message);
    }

    public static void main(String[] args) {
        try {
            init(args).run();
        } catch (Exception e) {
            exitWithError(LOGGER, "Failed to run testsuite", e);
        }
    }
}
