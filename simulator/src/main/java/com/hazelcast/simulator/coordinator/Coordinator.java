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

import static com.hazelcast.simulator.agent.workerprocess.WorkerProcessLauncher.WORKERS_HOME_NAME;
import static com.hazelcast.simulator.common.GitInfo.getBuildTime;
import static com.hazelcast.simulator.common.GitInfo.getCommitIdAbbrev;
import static com.hazelcast.simulator.coordinator.CoordinatorCli.init;
import static com.hazelcast.simulator.test.TestPhase.getTestPhaseSyncMap;
import static com.hazelcast.simulator.utils.AgentUtils.checkInstallation;
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
import static com.hazelcast.simulator.utils.FileUtils.newFile;
import static com.hazelcast.simulator.utils.FormatUtils.HORIZONTAL_RULER;
import static com.hazelcast.simulator.utils.FormatUtils.secondsToHuman;
import static com.hazelcast.simulator.utils.NativeUtils.execute;
import static com.hazelcast.simulator.utils.jars.HazelcastJARs.OUT_OF_THE_BOX;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;

public final class Coordinator {

    static final String SIMULATOR_VERSION = getSimulatorVersion();

    private static final int WAIT_FOR_WORKER_FAILURE_RETRY_COUNT = 10;

    private static final Logger LOGGER = Logger.getLogger(Coordinator.class);

    private final File outputDirectory = new File(System.getProperty("user.dir"));

    private final TestPhaseListeners testPhaseListeners = new TestPhaseListeners();
    private final PerformanceStateContainer performanceStateContainer = new PerformanceStateContainer();
    private final HdrHistogramContainer hdrHistogramContainer;

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
        this(testSuite, componentRegistry, coordinatorParameters, workerParameters, clusterLayoutParameters,
                new ClusterLayout(componentRegistry, workerParameters, clusterLayoutParameters));
    }

    Coordinator(TestSuite testSuite, ComponentRegistry componentRegistry, CoordinatorParameters coordinatorParameters,
                WorkerParameters workerParameters, ClusterLayoutParameters clusterLayoutParameters, ClusterLayout clusterLayout) {
        this.testSuite = testSuite;
        this.componentRegistry = componentRegistry;
        this.coordinatorParameters = coordinatorParameters;
        this.workerParameters = workerParameters;
        this.clusterLayoutParameters = clusterLayoutParameters;
        this.hdrHistogramContainer = new HdrHistogramContainer(outputDirectory, performanceStateContainer);
        this.failureContainer = new FailureContainer(testSuite, componentRegistry);

        this.simulatorProperties = coordinatorParameters.getSimulatorProperties();
        this.bash = new Bash(simulatorProperties);

        this.clusterLayout = clusterLayout;
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
    TestPhaseListeners getTestPhaseListeners() {
        return testPhaseListeners;
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

    void run() {
        try {
            checkInstallation(bash, simulatorProperties, componentRegistry);
            uploadFiles();

            try {
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
                    stopAgents(LOGGER, bash, simulatorProperties, componentRegistry);
                }
            }
        } finally {
            if (hazelcastJARs != null) {
                hazelcastJARs.shutdown();
            }
            moveLogFiles();
            OperationTypeCounter.printStatistics();
        }
    }

    void uploadFiles() {
        if (isLocal(simulatorProperties)) {
            if (!simulatorProperties.getHazelcastVersionSpec().equals(OUT_OF_THE_BOX)) {
                throw new CommandLineExitException("Local mode doesn't support custom Hazelcast versions!");
            }
        }

        Uploader uploader = new Uploader(
                isLocal(simulatorProperties),
                bash,
                componentRegistry,
                clusterLayout,
                hazelcastJARs,
                coordinatorParameters.isUploadHazelcastJARs(),
                coordinatorParameters.isEnterpriseEnabled(),
                coordinatorParameters.getWorkerClassPath(),
                testSuite.getId());
        uploader.run();
    }

    private void startCoordinatorConnector() {
        try {
            coordinatorConnector = new CoordinatorConnector(failureContainer, testPhaseListeners,
                    performanceStateContainer, hdrHistogramContainer);
            failureContainer.addListener(coordinatorConnector);
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
        int workerPingIntervalMillis = (int) SECONDS.toMillis(simulatorProperties.getWorkerPingIntervalSeconds());
        int shutdownDelaySeconds = simulatorProperties.getMemberWorkerShutdownDelaySeconds();

        remoteClient = new RemoteClient(
                coordinatorConnector,
                componentRegistry,
                workerPingIntervalMillis,
                shutdownDelaySeconds,
                coordinatorParameters.getWorkerVmStartupDelayMs()
        );
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
            boolean parallel = coordinatorParameters.isParallel() && testCount > 1;
            int maxTestCaseIdLength = testSuite.getMaxTestCaseIdLength();
            Map<TestPhase, CountDownLatch> testPhaseSyncMap = getTestPhaseSyncMap(testCount, parallel, lastTestPhaseToSync);

            echo("Starting TestSuite: %s", testSuite.getId());
            logTestSuiteDuration(parallel);

            for (TestData testData : componentRegistry.getTests()) {
                int testIndex = testData.getTestIndex();
                TestCase testCase = testData.getTestCase();
                echo("Configuration for %s (T%d):%n%s", testCase.getId(), testIndex, testCase);
                TestCaseRunner runner = new TestCaseRunner(testIndex, testCase, this, maxTestCaseIdLength, testPhaseSyncMap);
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
            echo("Terminating %d Workers...", runningWorkerCount);
            remoteClient.terminateWorkers(true);

            int waitForWorkerShutdownTimeoutSeconds = simulatorProperties.getWaitForWorkerShutdownTimeoutSeconds();
            if (!failureContainer.waitForWorkerShutdown(runningWorkerCount, waitForWorkerShutdownTimeoutSeconds)) {
                Set<SimulatorAddress> finishedWorkers = failureContainer.getFinishedWorkers();
                LOGGER.warn(format("Unfinished workers: %s", componentRegistry.getMissingWorkers(finishedWorkers).toString()));
            }

            performanceStateContainer.logDetailedPerformanceInfo(testSuite.getDurationSeconds());
            for (TestCase testCase : testSuite.getTestCaseList()) {
                hdrHistogramContainer.writeAggregatedHistograms(testSuite.getId(), testCase.getId());
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
                echo("Expected total TestSuite time: %s", secondsToHuman(totalDuration));
            }
        } else if (testSuite.isWaitForTestCase()) {
            echo("Testsuite will run until tests are finished");
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
                echo("Aborting TestSuite due to critical failure");
                break;
            }
            // restart Workers if needed, but not after last test
            if ((hasCriticalFailure || coordinatorParameters.isRefreshJvm()) && ++testIndex < testSuite.size()) {
                startWorkers();
            }
        }
    }

    private void moveLogFiles() {
        if (isLocal(simulatorProperties)) {
            File workerHome = newFile(getSimulatorHome(), WORKERS_HOME_NAME);
            File targetDirectory = ensureExistingDirectory(newFile(".", WORKERS_HOME_NAME), testSuite.getId());

            String workerPath = workerHome.getAbsolutePath();
            String targetPath = targetDirectory.getAbsolutePath();

            execute(format("mv %s/%s/* %s || true", workerPath, testSuite.getId(), targetPath));
            execute(format("rmdir %s/%s || true", workerPath, testSuite.getId()));
            execute(format("rmdir %s || true", workerPath));
            execute(format("mv ./agent.err %s/ || true", targetPath));
            execute(format("mv ./agent.out %s/ || true", targetPath));
            execute(format("mv ./failures-%s.txt %s/ || true", testSuite.getId(), targetPath));
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

    private void echo(String message, Object... args) {
        String log = echoLocal(message, args);
        remoteClient.logOnAllAgents(log);
    }

    public static void main(String[] args) {
        try {
            init(args).run();
        } catch (Exception e) {
            exitWithError(LOGGER, "Failed to run TestSuite", e);
        }
    }

    static void logHeader() {
        echoLocal("Hazelcast Simulator Coordinator");
        echoLocal("Version: %s, Commit: %s, Build Time: %s", SIMULATOR_VERSION, getCommitIdAbbrev(), getBuildTime());
        echoLocal("SIMULATOR_HOME: %s", getSimulatorHome().getAbsolutePath());
    }

    private static String echoLocal(String message, Object... args) {
        String log = message == null ? "null" : format(message, args);
        LOGGER.info(log);
        return log;
    }
}
