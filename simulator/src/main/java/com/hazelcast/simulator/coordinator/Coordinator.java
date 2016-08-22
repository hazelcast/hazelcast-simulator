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
import com.hazelcast.simulator.common.TestPhase;
import com.hazelcast.simulator.common.TestSuite;
import com.hazelcast.simulator.protocol.connector.CoordinatorConnector;
import com.hazelcast.simulator.protocol.operation.FailureOperation;
import com.hazelcast.simulator.protocol.operation.InitSessionOperation;
import com.hazelcast.simulator.protocol.operation.OperationTypeCounter;
import com.hazelcast.simulator.protocol.operation.StartWorkersOperation;
import com.hazelcast.simulator.protocol.operation.StopWorkersOperation;
import com.hazelcast.simulator.protocol.processors.CoordinatorOperationProcessor;
import com.hazelcast.simulator.protocol.registry.AgentData;
import com.hazelcast.simulator.protocol.registry.ComponentRegistry;
import com.hazelcast.simulator.utils.Bash;
import com.hazelcast.simulator.utils.CommandLineExitException;
import com.hazelcast.simulator.utils.ThreadSpawner;
import com.hazelcast.simulator.worker.WorkerType;
import org.apache.log4j.Logger;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;

import static com.hazelcast.simulator.coordinator.CoordinatorCli.loadClientHzConfig;
import static com.hazelcast.simulator.coordinator.CoordinatorCli.loadLog4jConfig;
import static com.hazelcast.simulator.coordinator.CoordinatorCli.loadMemberHzConfig;
import static com.hazelcast.simulator.coordinator.CoordinatorCli.loadWorkerScript;
import static com.hazelcast.simulator.coordinator.DeploymentPlan.createDeploymentPlan;
import static com.hazelcast.simulator.utils.AgentUtils.checkInstallation;
import static com.hazelcast.simulator.utils.AgentUtils.startAgents;
import static com.hazelcast.simulator.utils.AgentUtils.stopAgents;
import static com.hazelcast.simulator.utils.CommonUtils.closeQuietly;
import static com.hazelcast.simulator.utils.CommonUtils.exit;
import static com.hazelcast.simulator.utils.CommonUtils.sleepSeconds;
import static com.hazelcast.simulator.utils.FileUtils.ensureNewDirectory;
import static com.hazelcast.simulator.utils.FileUtils.getUserDir;
import static com.hazelcast.simulator.utils.HazelcastUtils.initClientHzConfig;
import static com.hazelcast.simulator.utils.HazelcastUtils.initMemberHzConfig;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

@SuppressWarnings("checkstyle:classdataabstractioncoupling")
public final class Coordinator {

    private static final int WAIT_FOR_WORKER_FAILURE_RETRY_COUNT = 10;

    private static final Logger LOGGER = Logger.getLogger(Coordinator.class);
    private static final int INTERACTIVE_MODE_INITIALIZE_TIMEOUT_MINUTES = 5;

    private final File outputDirectory;

    private final TestPhaseListeners testPhaseListeners = new TestPhaseListeners();
    private final PerformanceStatsCollector performanceStatsCollector = new PerformanceStatsCollector();

    private final ComponentRegistry componentRegistry;
    private final CoordinatorParameters coordinatorParameters;

    private final FailureCollector failureCollector;

    private final SimulatorProperties simulatorProperties;
    private final Bash bash;

    private final DeploymentPlan deploymentPlan;
    private final TestPhase lastTestPhaseToSync;

    private RemoteClient remoteClient;
    private CoordinatorConnector coordinatorConnector;

    private CountDownLatch interactiveModeInitialized = new CountDownLatch(1);

    Coordinator(ComponentRegistry componentRegistry,
                CoordinatorParameters coordinatorParameters,
                DeploymentPlan deploymentPlan) {

        this.outputDirectory = ensureNewDirectory(new File(getUserDir(), coordinatorParameters.getSessionId()));
        this.componentRegistry = componentRegistry;
        this.coordinatorParameters = coordinatorParameters;
        this.failureCollector = new FailureCollector(outputDirectory);
        this.failureCollector.addListener(true, new ComponentRegistryFailureListener(componentRegistry));
        this.simulatorProperties = coordinatorParameters.getSimulatorProperties();
        this.bash = new Bash(simulatorProperties);
        this.deploymentPlan = deploymentPlan;
        this.lastTestPhaseToSync = coordinatorParameters.getLastTestPhaseToSync();
    }

    private void logConfiguration() {
        echoLocal("Total number of agents: %s", componentRegistry.agentCount());
        echoLocal("Total number of Hazelcast member workers: %s", deploymentPlan.getMemberWorkerCount());
        echoLocal("Total number of Hazelcast client workers: %s", deploymentPlan.getClientWorkerCount());
        echoLocal("Last TestPhase to sync: %s", lastTestPhaseToSync);
        echoLocal("Output directory: " + outputDirectory.getAbsolutePath());

        int performanceIntervalSeconds = coordinatorParameters.getPerformanceMonitorIntervalSeconds();
        if (performanceIntervalSeconds > 0) {
            echoLocal("Performance monitor enabled (%d seconds)", performanceIntervalSeconds);
        } else {
            echoLocal("Performance monitor disabled");
        }
    }

    void run(TestSuite testSuite) {
        logConfiguration();

        checkInstallation(bash, simulatorProperties, componentRegistry);
        new InstallVendorTask(
                simulatorProperties,
                componentRegistry.getAgentIps(),
                deploymentPlan.getVersionSpecs(),
                coordinatorParameters.getSessionId()).run();

        try {
            try {
                startAgents(LOGGER, bash, simulatorProperties, componentRegistry);
                startCoordinatorConnector();
                startRemoteClient();
                new StartWorkersTask(
                        deploymentPlan.getWorkerDeployment(),
                        remoteClient,
                        componentRegistry,
                        coordinatorParameters.getWorkerVmStartupDelayMs()).run();

                new RunTestSuiteTask(testSuite,
                        coordinatorParameters,
                        componentRegistry,
                        failureCollector,
                        testPhaseListeners,
                        remoteClient,
                        performanceStatsCollector).run();
            } catch (CommandLineExitException e) {
                for (int i = 0; i < WAIT_FOR_WORKER_FAILURE_RETRY_COUNT && failureCollector.getFailureCount() == 0; i++) {
                    sleepSeconds(1);
                }
                throw e;
            } finally {
                new TerminateWorkersTask(simulatorProperties, componentRegistry, remoteClient).run();
                try {
                    failureCollector.logFailureInfo();
                } finally {
                    if (coordinatorConnector != null) {
                        echo("Shutdown of ClientConnector...");
                        coordinatorConnector.shutdown();
                    }
                    stopAgents(LOGGER, bash, simulatorProperties, componentRegistry);
                }
            }
        } finally {
            close();
        }
    }

    private void close() {
        closeQuietly(remoteClient);

        if (!coordinatorParameters.skipDownload()) {
            new DownloadTask(
                    coordinatorParameters.getSessionId(),
                    simulatorProperties,
                    outputDirectory,
                    componentRegistry).run();
        }
        executeAfterCompletion();

        OperationTypeCounter.printStatistics();
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
            CoordinatorOperationProcessor processor = new CoordinatorOperationProcessor(
                    this, failureCollector, testPhaseListeners, performanceStatsCollector);

            coordinatorConnector = new CoordinatorConnector(processor, simulatorProperties.getCoordinatorPort());
            coordinatorConnector.start();

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

    void startInteractive() {
        echoLocal("Coordinator interactive mode starting...");

        checkInstallation(bash, simulatorProperties, componentRegistry);

        startAgents(LOGGER, bash, simulatorProperties, componentRegistry);
        startCoordinatorConnector();
        startRemoteClient();

        new InstallVendorTask(
                simulatorProperties,
                componentRegistry.getAgentIps(),
                deploymentPlan.getVersionSpecs(),
                coordinatorParameters.getSessionId()).run();


        echoLocal("Coordinator interactive mode started...");
        echoLocal("Total number of agents: %s", componentRegistry.agentCount());
        echoLocal("Output directory: " + outputDirectory.getAbsolutePath());
        int performanceIntervalSeconds = coordinatorParameters.getPerformanceMonitorIntervalSeconds();
        if (performanceIntervalSeconds > 0) {
            echoLocal("Performance monitor enabled (%d seconds)", performanceIntervalSeconds);
        } else {
            echoLocal("Performance monitor disabled");
        }

        interactiveModeInitialized.countDown();
    }

    private void awaitInteractiveModeInitialized() throws Exception {
        if (!interactiveModeInitialized.await(INTERACTIVE_MODE_INITIALIZE_TIMEOUT_MINUTES, MINUTES)) {
            throw new TimeoutException("Coordinator interactive mode failed to complete");
        }
    }

    private void startRemoteClient() {
        LOGGER.info("Remote client starting....");
        int workerPingIntervalMillis = (int) SECONDS.toMillis(simulatorProperties.getWorkerPingIntervalSeconds());

        remoteClient = new RemoteClient(coordinatorConnector, componentRegistry, workerPingIntervalMillis);
        remoteClient.sendToAllAgents(new InitSessionOperation(coordinatorParameters.getSessionId()));
        LOGGER.info("Remote client started successfully!");
    }

    private void echo(String message, Object... args) {
        String log = echoLocal(message, args);
        remoteClient.logOnAllAgents(log);
    }

    private static String echoLocal(String message, Object... args) {
        String log = message == null ? "null" : format(message, args);
        LOGGER.info(log);
        return log;
    }

    public void installVendor(String versionSpec) throws Exception {
        awaitInteractiveModeInitialized();

        LOGGER.info("Installing versionSpec [" + versionSpec + "] on " + componentRegistry.getAgents().size() + " agents ....");
        new InstallVendorTask(
                simulatorProperties,
                componentRegistry.getAgentIps(),
                Collections.singleton(versionSpec),
                coordinatorParameters.getSessionId()).run();
        LOGGER.info("Install successful!");
    }

    public void shutdown() {
        LOGGER.info("Shutting down....");

        new TerminateWorkersTask(simulatorProperties, componentRegistry, remoteClient).run();

        close();

        new Thread(new Runnable() {
            private static final int DELAY = 5000;

            @Override
            public void run() {
                try {
                    Thread.sleep(DELAY);

                    if (coordinatorConnector != null) {
                        echo("Shutdown of ClientConnector...");
                        coordinatorConnector.shutdown();
                    }
                    exit(0);
                } catch (Exception e) {
                    LOGGER.warn("Failed to shutdown", e);
                }
            }
        }).start();
    }

    public void stopWorkers(StopWorkersOperation op) throws Exception {
        awaitInteractiveModeInitialized();

        LOGGER.info("Stopping workers...");

        new TerminateWorkersTask(simulatorProperties, componentRegistry, remoteClient).run();

        LOGGER.info("Stopping workers complete!");
    }

    public void startWorkers(StartWorkersOperation op) throws Exception {
        awaitInteractiveModeInitialized();

        WorkerType workerType = new WorkerType(op.getWorkerType());

        LOGGER.info("Starting [" + workerType + "] workers: " + op.getCount());

        Map<String, String> environment = new HashMap<String, String>();
        environment.put("AUTOCREATE_HAZELCAST_INSTANCE", "true");
        environment.put("LOG4j_CONFIG", loadLog4jConfig());
        environment.put("JVM_OPTIONS", op.getVmOptions());
        environment.put("WORKER_PERFORMANCE_MONITOR_INTERVAL_SECONDS",
                "" + coordinatorParameters.getPerformanceMonitorIntervalSeconds());

        String config;
        if (WorkerType.MEMBER.equals(workerType)) {
            config = initMemberHzConfig(
                    op.getHzConfig() == null ? loadMemberHzConfig() : op.getHzConfig(),
                    componentRegistry,
                    simulatorProperties.getHazelcastPort(),
                    "",
                    simulatorProperties, false);
        } else if (WorkerType.LITE_MEMBER.equals(workerType)) {
            config = initMemberHzConfig(
                    op.getHzConfig() == null ? loadMemberHzConfig() : op.getHzConfig(),
                    componentRegistry,
                    simulatorProperties.getHazelcastPort(),
                    "",
                    simulatorProperties, true);

            LOGGER.info(config);
        } else if (WorkerType.JAVA_CLIENT.equals(workerType)) {
            config = initClientHzConfig(
                    op.getHzConfig() == null ? loadClientHzConfig() : op.getHzConfig(),
                    componentRegistry,
                    simulatorProperties.getHazelcastPort(),
                    "");

        } else {
            throw new IllegalStateException("Unrecognized workerType [" + workerType + "]");
        }

        environment.put("HAZELCAST_CONFIG", config);

        String versionSpec = op.getVersionSpec() == null
                ? simulatorProperties.getVersionSpec()
                : op.getVersionSpec();

        WorkerParameters workerParameters = new WorkerParameters(
                versionSpec,
                simulatorProperties.getAsInt("WORKER_STARTUP_TIMEOUT_SECONDS"),
                loadWorkerScript(workerType, simulatorProperties.get("VENDOR")),
                environment);

        DeploymentPlan deploymentPlan = createDeploymentPlan(
                componentRegistry,
                workerParameters,
                workerType,
                op.getCount(),
                0); //todo:dedicated machines.. we don't know here.

        new StartWorkersTask(
                deploymentPlan.getWorkerDeployment(),
                remoteClient,
                componentRegistry,
                coordinatorParameters.getWorkerVmStartupDelayMs()).run();

        LOGGER.info("Workers started!");
    }

    public void runSuite(TestSuite testSuite) throws Exception {
        awaitInteractiveModeInitialized();

        LOGGER.info("Run starting!");

        new RunTestSuiteTask(testSuite,
                coordinatorParameters,
                componentRegistry,
                failureCollector,
                testPhaseListeners,
                remoteClient,
                performanceStatsCollector).run();

        LOGGER.info("Run complete!");
    }

    private static class ComponentRegistryFailureListener implements FailureListener {

        private final ComponentRegistry componentRegistry;

        ComponentRegistryFailureListener(ComponentRegistry componentRegistry) {
            this.componentRegistry = componentRegistry;
        }

        @Override
        public void onFailure(FailureOperation failure, boolean isFinishedFailure, boolean isCritical) {
            FailureType failureType = failure.getType();

            if (failureType.isWorkerFinishedFailure()) {
                componentRegistry.removeWorker(failure.getWorkerAddress());
            }
        }
    }
}
