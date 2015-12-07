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

import com.hazelcast.simulator.cluster.ClusterLayout;
import com.hazelcast.simulator.common.SimulatorProperties;
import com.hazelcast.simulator.protocol.connector.CoordinatorConnector;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.registry.AgentData;
import com.hazelcast.simulator.protocol.registry.ComponentRegistry;
import com.hazelcast.simulator.protocol.registry.TestData;
import com.hazelcast.simulator.test.TestCase;
import com.hazelcast.simulator.test.TestPhase;
import com.hazelcast.simulator.test.TestSuite;
import com.hazelcast.simulator.utils.Bash;
import com.hazelcast.simulator.utils.CommandLineExitException;
import com.hazelcast.simulator.utils.ThreadSpawner;
import com.hazelcast.simulator.utils.jars.HazelcastJARs;
import org.apache.log4j.Logger;

import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;

import static com.hazelcast.simulator.coordinator.CoordinatorCli.init;
import static com.hazelcast.simulator.coordinator.FailureContainer.FINISHED_WORKER_TIMEOUT_SECONDS;
import static com.hazelcast.simulator.test.TestPhase.getTestPhaseSyncMap;
import static com.hazelcast.simulator.utils.CloudProviderUtils.isEC2;
import static com.hazelcast.simulator.utils.CommonUtils.exitWithError;
import static com.hazelcast.simulator.utils.CommonUtils.getElapsedSeconds;
import static com.hazelcast.simulator.utils.CommonUtils.getSimulatorVersion;
import static com.hazelcast.simulator.utils.CommonUtils.rethrow;
import static com.hazelcast.simulator.utils.CommonUtils.sleepSeconds;
import static com.hazelcast.simulator.utils.FormatUtils.HORIZONTAL_RULER;
import static com.hazelcast.simulator.utils.FormatUtils.secondsToHuman;
import static com.hazelcast.simulator.utils.HarakiriMonitorUtils.getStartHarakiriMonitorCommandOrNull;
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

        boolean performanceEnabled = workerParameters.isMonitorPerformance();
        int performanceIntervalSeconds = workerParameters.getWorkerPerformanceMonitorIntervalSeconds();
        echoLocal("Performance monitor enabled: %s (%d seconds)", performanceEnabled, performanceIntervalSeconds);
    }

    private void run() throws Exception {
        try {
            uploadFiles();

            startAgents();
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
                shutdown();
            }
        }
    }

    private void uploadFiles() {
        CoordinatorUploader uploader = new CoordinatorUploader(bash, componentRegistry, clusterLayout, hazelcastJARs,
                coordinatorParameters.isUploadHazelcastJARs(), coordinatorParameters.isEnterpriseEnabled(),
                coordinatorParameters.getWorkerClassPath(), workerParameters.getProfiler(), testSuite.getId());
        uploader.run();
    }

    private void startAgents() {
        echoLocal("Starting %s Agents", componentRegistry.agentCount());
        ThreadSpawner spawner = new ThreadSpawner("startAgents", true);
        final int agentPort = simulatorProperties.getAgentPort();
        for (final AgentData agentData : componentRegistry.getAgents()) {
            spawner.spawn(new Runnable() {
                @Override
                public void run() {
                    startAgent(agentData.getAddressIndex(), agentData.getPublicAddress(), agentPort);
                }
            });
        }
        spawner.awaitCompletion();
        echoLocal("Successfully started agents on %s boxes", componentRegistry.agentCount());

        try {
            startCoordinatorConnector();
        } catch (Exception e) {
            throw new CommandLineExitException("Could not start CoordinatorConnector", e);
        }

        remoteClient = new RemoteClient(coordinatorConnector, componentRegistry);
        remoteClient.initTestSuite(testSuite);
    }

    private void startAgent(int addressIndex, String ip, int port) {
        echoLocal("Killing Java processes on %s", ip);
        bash.killAllJavaProcesses(ip);

        echoLocal("Starting Agent on %s", ip);
        String mandatoryParameters = format("--addressIndex %d --publicAddress %s --port %s", addressIndex, ip, port);
        String optionalParameters = "";
        if (isEC2(simulatorProperties)) {
            optionalParameters = format(" --cloudProvider %s --cloudIdentity %s --cloudCredential %s",
                    simulatorProperties.getCloudProvider(),
                    simulatorProperties.getCloudIdentity(),
                    simulatorProperties.getCloudCredential());
        }
        bash.ssh(ip, format("nohup hazelcast-simulator-%s/bin/agent %s%s > agent.out 2> agent.err < /dev/null &",
                SIMULATOR_VERSION, mandatoryParameters, optionalParameters));

        bash.ssh(ip, format("hazelcast-simulator-%s/bin/.await-file-exists agent.pid", SIMULATOR_VERSION));
    }

    private void startCoordinatorConnector() {
        coordinatorConnector = new CoordinatorConnector(testPhaseListenerContainer, performanceStateContainer,
                testHistogramContainer, failureContainer);
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
    }

    private void startWorkers() {
        try {
            long started = System.nanoTime();

            echo(HORIZONTAL_RULER);
            echo("Starting Workers...");
            echo(HORIZONTAL_RULER);

            echo("Killing all remaining Workers...");
            remoteClient.terminateWorkers(false);
            echo("Successfully killed all remaining Workers");

            int totalWorkerCount = clusterLayout.getTotalMemberCount();
            echo("Starting %d Workers (%d members, %d clients)...", totalWorkerCount, clusterLayout.getMemberWorkerCount(),
                    clusterLayout.getClientWorkerCount());
            remoteClient.createWorkers(clusterLayout, true);

            long elapsed = getElapsedSeconds(started);
            echo(HORIZONTAL_RULER);
            LOGGER.info((format("Finished starting of %s Worker JVMs (%s seconds)", totalWorkerCount, elapsed)));
            echo(HORIZONTAL_RULER);
        } catch (Exception e) {
            throw new CommandLineExitException("Failed to start Workers", e);
        }
    }

    void runTestSuite() {
        try {
            boolean isParallel = coordinatorParameters.isParallel();
            int testCount = testSuite.size();
            int maxTestCaseIdLength = testSuite.getMaxTestCaseIdLength();

            TestPhase lastTestPhaseToSync = coordinatorParameters.getLastTestPhaseToSync();
            ConcurrentMap<TestPhase, CountDownLatch> testPhaseSyncs = getTestPhaseSyncMap(isParallel, testCount,
                    lastTestPhaseToSync);

            echo("Starting testsuite: %s", testSuite.getId());
            logTestSuiteDuration();

            for (TestData testData : componentRegistry.getTests()) {
                int testIndex = testData.getTestIndex();
                TestCase testCase = testData.getTestCase();
                echo("Configuration for %s (T%d):%n%s", testCase.getId(), testIndex, testCase);
                TestCaseRunner runner = new TestCaseRunner(testIndex, testCase, this, maxTestCaseIdLength, testPhaseSyncs);
                testPhaseListenerContainer.addListener(testIndex, runner);
            }

            echo(HORIZONTAL_RULER);
            echo("Running %s tests (%s)", testCount, isParallel ? "parallel" : "sequentially");
            echo(HORIZONTAL_RULER);
            long started = System.nanoTime();
            if (isParallel) {
                runParallel();
            } else {
                runSequential();
            }
            echo(HORIZONTAL_RULER);
            echo("Finished running of %d tests (%s)", testCount, secondsToHuman(getElapsedSeconds(started)));
            echo(HORIZONTAL_RULER);
        } finally {
            remoteClient.terminateWorkers(true);
            if (!failureContainer.waitForWorkerShutdown(componentRegistry.workerCount(), FINISHED_WORKER_TIMEOUT_SECONDS)) {
                Set<SimulatorAddress> finishedWorkers = failureContainer.getFinishedWorkers();
                LOGGER.warn(format("Unfinished workers: %s", componentRegistry.getMissingWorkers(finishedWorkers).toString()));
            }

            performanceStateContainer.logDetailedPerformanceInfo();
            for (TestCase testCase : testSuite.getTestCaseList()) {
                testHistogramContainer.createProbeResults(testSuite.getId(), testCase.getId());
            }
        }
    }

    private void logTestSuiteDuration() {
        int testDuration = testSuite.getDurationSeconds();
        if (testDuration > 0) {
            echo("Running time per test: %s", secondsToHuman(testDuration));
            int totalDuration = (coordinatorParameters.isParallel()) ? testDuration : testDuration * testSuite.size();
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

    private void runSequential() {
        for (TestPhaseListener testCaseRunner : testPhaseListenerContainer.getListeners()) {
            ((TestCaseRunner) testCaseRunner).run();
            boolean hasCriticalFailure = failureContainer.hasCriticalFailure();
            if (hasCriticalFailure && testSuite.isFailFast()) {
                LOGGER.info("Aborting testsuite due to critical failure");
                break;
            }
            if (hasCriticalFailure || coordinatorParameters.isRefreshJvm()) {
                startWorkers();
            }
        }
    }

    private void shutdown() throws Exception {
        if (coordinatorConnector != null) {
            LOGGER.info("Shutdown of ClientConnector...");
            coordinatorConnector.shutdown();
        }

        stopAgents();
    }

    private void stopAgents() {
        final String startHarakiriMonitorCommand = getStartHarakiriMonitorCommandOrNull(simulatorProperties);

        echoLocal("Stopping %s Agents", componentRegistry.agentCount());
        ThreadSpawner spawner = new ThreadSpawner("killAgents", true);
        for (final AgentData agentData : componentRegistry.getAgents()) {
            spawner.spawn(new Runnable() {
                @Override
                public void run() {
                    String ip = agentData.getPublicAddress();
                    echoLocal("Stopping Agent %s", ip);
                    bash.ssh(ip, format("hazelcast-simulator-%s/bin/.kill-from-pid-file agent.pid", SIMULATOR_VERSION));

                    if (startHarakiriMonitorCommand != null) {
                        LOGGER.info(format("Starting HarakiriMonitor on %s", ip));
                        bash.ssh(ip, startHarakiriMonitorCommand);
                    }
                }
            });
        }
        spawner.awaitCompletion();
        echoLocal("Successfully stopped %s Agents", componentRegistry.agentCount());
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
