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

import com.hazelcast.simulator.common.SimulatorProperties;
import com.hazelcast.simulator.common.TestPhase;
import com.hazelcast.simulator.coordinator.tasks.ArtifactDownloadTask;
import com.hazelcast.simulator.coordinator.tasks.InstallVendorTask;
import com.hazelcast.simulator.coordinator.tasks.RunTestSuiteTask;
import com.hazelcast.simulator.coordinator.tasks.StartWorkersTask;
import com.hazelcast.simulator.coordinator.tasks.TerminateWorkersTask;
import com.hazelcast.simulator.protocol.connector.CoordinatorConnector;
import com.hazelcast.simulator.protocol.operation.InitSessionOperation;
import com.hazelcast.simulator.protocol.operation.OperationTypeCounter;
import com.hazelcast.simulator.protocol.processors.CoordinatorOperationProcessor;
import com.hazelcast.simulator.protocol.registry.AgentData;
import com.hazelcast.simulator.protocol.registry.ComponentRegistry;
import com.hazelcast.simulator.utils.Bash;
import com.hazelcast.simulator.utils.CommandLineExitException;
import com.hazelcast.simulator.utils.ThreadSpawner;
import org.apache.log4j.Logger;

import java.io.Closeable;
import java.io.File;

import static com.hazelcast.simulator.utils.AgentUtils.checkInstallation;
import static com.hazelcast.simulator.utils.AgentUtils.startAgents;
import static com.hazelcast.simulator.utils.AgentUtils.stopAgents;
import static com.hazelcast.simulator.utils.CommonUtils.closeQuietly;
import static com.hazelcast.simulator.utils.CommonUtils.sleepSeconds;
import static com.hazelcast.simulator.utils.FileUtils.ensureNewDirectory;
import static com.hazelcast.simulator.utils.FileUtils.getUserDir;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;

@SuppressWarnings({"checkstyle:classdataabstractioncoupling", "checkstyle:classfanoutcomplexity"})
public class CoordinatorRun implements Closeable {
    private static final int WAIT_FOR_WORKER_FAILURE_RETRY_COUNT = 10;

    private static final Logger LOGGER = Logger.getLogger(CoordinatorRun.class);

    private final TestPhaseListeners testPhaseListeners = new TestPhaseListeners();
    private final PerformanceStatsCollector performanceStatsCollector = new PerformanceStatsCollector();

    private final ComponentRegistry componentRegistry;
    private final CoordinatorParameters coordinatorParameters;

    private final FailureCollector failureCollector;

    private final SimulatorProperties simulatorProperties;
    private final Bash bash;

    private final TestPhase lastTestPhaseToSync;
    private final File outputDirectory;

    private RemoteClient client;
    private CoordinatorConnector connector;

    CoordinatorRun(ComponentRegistry componentRegistry,
                   CoordinatorParameters coordinatorParameters) {

        this.outputDirectory = ensureNewDirectory(new File(getUserDir(), coordinatorParameters.getSessionId()));
        this.componentRegistry = componentRegistry;
        this.coordinatorParameters = coordinatorParameters;
        this.failureCollector = new FailureCollector(outputDirectory, componentRegistry);
        this.simulatorProperties = coordinatorParameters.getSimulatorProperties();
        this.bash = new Bash(simulatorProperties);
        this.lastTestPhaseToSync = coordinatorParameters.getLastTestPhaseToSync();
    }

    public void init(DeploymentPlan deploymentPlan) {
        logConfiguration(deploymentPlan);
        checkInstallation(bash, simulatorProperties, componentRegistry);

        new InstallVendorTask(simulatorProperties,
                componentRegistry.getAgentIps(),
                deploymentPlan.getVersionSpecs(),
                coordinatorParameters.getSessionId()).run();

        startAgents(LOGGER, bash, simulatorProperties, componentRegistry);
        startCoordinatorConnector();

        new StartWorkersTask(
                deploymentPlan.getWorkerDeployment(),
                client,
                componentRegistry,
                coordinatorParameters.getWorkerVmStartupDelayMs()).run();
    }

    public boolean run(TestSuite testSuite) throws Exception {
        try {
            new RunTestSuiteTask(testSuite,
                    coordinatorParameters,
                    componentRegistry,
                    failureCollector,
                    testPhaseListeners,
                    client,
                    performanceStatsCollector).run();
        } catch (CommandLineExitException e) {
            for (int i = 0; i < WAIT_FOR_WORKER_FAILURE_RETRY_COUNT && failureCollector.getFailureCount() == 0; i++) {
                sleepSeconds(1);
            }
            throw e;
        }
        return !failureCollector.hasCriticalFailure();
    }

    private void logConfiguration(DeploymentPlan deploymentPlan) {
        LOGGER.info(format("Total number of agents: %s", componentRegistry.agentCount()));
        LOGGER.info(format("Total number of Hazelcast member workers: %s", deploymentPlan.getMemberWorkerCount()));
        LOGGER.info(format("Total number of Hazelcast client workers: %s", deploymentPlan.getClientWorkerCount()));
        LOGGER.info(format("Last TestPhase to sync: %s", lastTestPhaseToSync));
        LOGGER.info(format("Output directory: %s", outputDirectory.getAbsolutePath()));

        int performanceIntervalSeconds = coordinatorParameters.getPerformanceMonitorIntervalSeconds();
        if (performanceIntervalSeconds > 0) {
            LOGGER.info(format("Performance monitor enabled (%d seconds)", performanceIntervalSeconds));
        } else {
            LOGGER.info("Performance monitor disabled");
        }
    }

    private void startCoordinatorConnector() {
        CoordinatorOperationProcessor processor = new CoordinatorOperationProcessor(
                null, failureCollector, testPhaseListeners, performanceStatsCollector);

        connector = new CoordinatorConnector(processor, simulatorProperties.getCoordinatorPort());
        connector.start();

        ThreadSpawner spawner = new ThreadSpawner("startCoordinatorConnector", true);
        for (final AgentData agentData : componentRegistry.getAgents()) {
            final int agentPort = simulatorProperties.getAgentPort();
            spawner.spawn(new Runnable() {
                @Override
                public void run() {
                    connector.addAgent(agentData.getAddressIndex(), agentData.getPublicAddress(), agentPort);
                    LOGGER.info(agentData.getAddress() + " added");
                }
            });
        }
        spawner.awaitCompletion();

        LOGGER.info("Remote client starting....");
        int workerPingIntervalMillis = (int) SECONDS.toMillis(simulatorProperties.getWorkerPingIntervalSeconds());

        client = new RemoteClient(connector, componentRegistry, workerPingIntervalMillis);
        client.invokeOnAllAgents(new InitSessionOperation(coordinatorParameters.getSessionId()));
        LOGGER.info("Remote client started successfully!");
    }

    @Override
    public void close() {
        new TerminateWorkersTask(simulatorProperties, componentRegistry, client).run();

        closeQuietly(connector);

        closeQuietly(client);

        stopAgents(LOGGER, bash, simulatorProperties, componentRegistry);

        if (!coordinatorParameters.skipDownload()) {
            new ArtifactDownloadTask(
                    coordinatorParameters.getSessionId(),
                    simulatorProperties,
                    outputDirectory,
                    componentRegistry).run();
            if (coordinatorParameters.getAfterCompletionFile() != null) {
                LOGGER.info("Executing after-completion script: " + coordinatorParameters.getAfterCompletionFile());
                bash.execute(coordinatorParameters.getAfterCompletionFile() + " " + outputDirectory.getAbsolutePath());
                LOGGER.info("Finished after-completion script");
            }
        }

        failureCollector.logFailureInfo();

        OperationTypeCounter.printStatistics();
    }
}
