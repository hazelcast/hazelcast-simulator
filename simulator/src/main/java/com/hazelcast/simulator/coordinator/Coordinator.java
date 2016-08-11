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

import com.hazelcast.simulator.common.FailureType;
import com.hazelcast.simulator.common.SimulatorProperties;
import com.hazelcast.simulator.common.TestSuite;
import com.hazelcast.simulator.coordinator.deployment.DeploymentPlan;
import com.hazelcast.simulator.protocol.connector.CoordinatorConnector;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.operation.FailureOperation;
import com.hazelcast.simulator.protocol.operation.InitTestSuiteOperation;
import com.hazelcast.simulator.protocol.operation.OperationTypeCounter;
import com.hazelcast.simulator.protocol.registry.AgentData;
import com.hazelcast.simulator.protocol.registry.ComponentRegistry;
import com.hazelcast.simulator.testcontainer.TestPhase;
import com.hazelcast.simulator.utils.Bash;
import com.hazelcast.simulator.utils.CommandLineExitException;
import com.hazelcast.simulator.utils.ThreadSpawner;
import org.apache.log4j.Logger;

import java.io.File;

import static com.hazelcast.simulator.common.GitInfo.getBuildTime;
import static com.hazelcast.simulator.common.GitInfo.getCommitIdAbbrev;
import static com.hazelcast.simulator.coordinator.CoordinatorCli.init;
import static com.hazelcast.simulator.utils.AgentUtils.checkInstallation;
import static com.hazelcast.simulator.utils.AgentUtils.startAgents;
import static com.hazelcast.simulator.utils.AgentUtils.stopAgents;
import static com.hazelcast.simulator.utils.CommonUtils.exitWithError;
import static com.hazelcast.simulator.utils.CommonUtils.getSimulatorVersion;
import static com.hazelcast.simulator.utils.CommonUtils.sleepSeconds;
import static com.hazelcast.simulator.utils.FileUtils.ensureExistingDirectory;
import static com.hazelcast.simulator.utils.FileUtils.getSimulatorHome;
import static com.hazelcast.simulator.utils.FileUtils.getUserDir;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;

@SuppressWarnings("checkstyle:classdataabstractioncoupling")
public final class Coordinator {

    private static final int WAIT_FOR_WORKER_FAILURE_RETRY_COUNT = 10;
    private static final String SIMULATOR_VERSION = getSimulatorVersion();

    private static final Logger LOGGER = Logger.getLogger(Coordinator.class);

    private final File outputDirectory;

    private final TestPhaseListeners testPhaseListeners = new TestPhaseListeners();
    private final PerformanceStatsContainer performanceStatsContainer = new PerformanceStatsContainer();

    private final TestSuite testSuite;
    private final ComponentRegistry componentRegistry;
    private final CoordinatorParameters coordinatorParameters;
    private final WorkerParameters workerParameters;
    private final ClusterLayoutParameters clusterLayoutParameters;

    private final FailureContainer failureContainer;

    private final SimulatorProperties simulatorProperties;
    private final Bash bash;

    private final DeploymentPlan deploymentPlan;
    private final TestPhase lastTestPhaseToSync;

    private RemoteClient remoteClient;
    private CoordinatorConnector coordinatorConnector;

    public Coordinator(TestSuite testSuite,
                       ComponentRegistry componentRegistry,
                       CoordinatorParameters coordinatorParameters,
                       WorkerParameters workerParameters,
                       ClusterLayoutParameters clusterLayoutParameters) {
        this(testSuite, componentRegistry, coordinatorParameters, workerParameters, clusterLayoutParameters,
                new DeploymentPlan(componentRegistry, workerParameters, clusterLayoutParameters));
    }

    Coordinator(TestSuite testSuite,
                final ComponentRegistry componentRegistry,
                CoordinatorParameters coordinatorParameters,
                WorkerParameters workerParameters,
                ClusterLayoutParameters clusterLayoutParameters,
                DeploymentPlan deploymentPlan) {

        this.outputDirectory = ensureExistingDirectory(new File(getUserDir(), testSuite.getId()));

        this.testSuite = testSuite;
        this.componentRegistry = componentRegistry;
        this.coordinatorParameters = coordinatorParameters;
        this.workerParameters = workerParameters;
        this.clusterLayoutParameters = clusterLayoutParameters;

        this.failureContainer = new FailureContainer(outputDirectory, testSuite.getTolerableFailures());
        this.failureContainer.addListener(new FailureListener() {
            @Override
            public void onFailure(FailureOperation failure, boolean isFinishedFailure, boolean isCritical) {
                FailureType failureType = failure.getType();

                if (failureType.isWorkerFinishedFailure()) {
                    SimulatorAddress workerAddress = failure.getWorkerAddress();
                    componentRegistry.removeWorker(workerAddress);
                }
            }
        });

        this.simulatorProperties = coordinatorParameters.getSimulatorProperties();
        this.bash = new Bash(simulatorProperties);

        this.deploymentPlan = deploymentPlan;
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

    private void logConfiguration() {
        echoLocal("Total number of agents: %s", componentRegistry.agentCount());
        echoLocal("Total number of Hazelcast member workers: %s", deploymentPlan.getMemberWorkerCount());
        echoLocal("Total number of Hazelcast client workers: %s", deploymentPlan.getClientWorkerCount());
        echoLocal("Last TestPhase to sync: %s", lastTestPhaseToSync);
        echoLocal("Output directory: " + outputDirectory.getAbsolutePath());

        boolean performanceEnabled = workerParameters.isMonitorPerformance();
        int performanceIntervalSeconds = workerParameters.getWorkerPerformanceMonitorIntervalSeconds();
        echoLocal("Performance monitor enabled: %s (%d seconds)", performanceEnabled, performanceIntervalSeconds);
    }

    void run() {
        boolean isPrePhaseDone = false;
        try {
            checkInstallation(bash, simulatorProperties, componentRegistry);
            new InstallVendorTask(
                    simulatorProperties,
                    componentRegistry.getAgentIps(),
                    deploymentPlan.getVersionSpecs(),
                    testSuite.getId()).run();
            isPrePhaseDone = true;

            try {
                startAgents(LOGGER, bash, simulatorProperties, componentRegistry);
                startCoordinatorConnector();
                startRemoteClient();
                new StartWorkersTask(
                        deploymentPlan.asMap(),
                        remoteClient,
                        componentRegistry,
                        coordinatorParameters.getWorkerVmStartupDelayMs()).run();

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
            if (isPrePhaseDone) {
                if (!coordinatorParameters.skipDownload()) {
                    new DownloadTask(testSuite.getId(), simulatorProperties, outputDirectory, componentRegistry).run();
                }
                executeAfterCompletion();

                OperationTypeCounter.printStatistics();
            }
        }
    }

    private void runTestSuite() {
        new RunTestSuiteTask(testSuite,
                coordinatorParameters,
                componentRegistry,
                failureContainer,
                testPhaseListeners,
                simulatorProperties,
                remoteClient,
                performanceStatsContainer,
                workerParameters).run();
    }

    private void executeAfterCompletion() {
        if (coordinatorParameters.getAfterCompletionFile() != null) {
            echoLocal("Executing after-completion script: " + coordinatorParameters.getAfterCompletionFile());
            bash.execute(coordinatorParameters.getAfterCompletionFile() + " " + outputDirectory.getAbsolutePath());
            echoLocal("Finished after-completion script");
        }
    }

    private void startCoordinatorConnector() {
        try {
            int coordinatorPort = simulatorProperties.getCoordinatorPort();
            coordinatorConnector = CoordinatorConnector.createInstance(componentRegistry, failureContainer,
                    testPhaseListeners, performanceStatsContainer, coordinatorPort);
            coordinatorConnector.start();
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
                shutdownDelaySeconds);

        remoteClient.sendToAllAgents(new InitTestSuiteOperation(testSuite));
    }

    private void echo(String message, Object... args) {
        String log = echoLocal(message, args);
        remoteClient.logOnAllAgents(log);
    }

    public static void main(String[] args) {
        try {
            init(args).run();
        } catch (Exception e) {
            exitWithError(LOGGER, "Failed to run Coordinator", e);
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
