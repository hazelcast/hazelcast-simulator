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
import com.hazelcast.simulator.common.WorkerType;
import com.hazelcast.simulator.coordinator.tasks.DownloadTask;
import com.hazelcast.simulator.coordinator.tasks.InstallVendorTask;
import com.hazelcast.simulator.coordinator.tasks.KillWorkersTask;
import com.hazelcast.simulator.coordinator.tasks.RunTestSuiteTask;
import com.hazelcast.simulator.coordinator.tasks.StartWorkersTask;
import com.hazelcast.simulator.coordinator.tasks.TerminateWorkersTask;
import com.hazelcast.simulator.protocol.connector.CoordinatorConnector;
import com.hazelcast.simulator.protocol.core.ResponseType;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.operation.ExecuteScriptOperation;
import com.hazelcast.simulator.protocol.operation.InitSessionOperation;
import com.hazelcast.simulator.protocol.operation.OperationTypeCounter;
import com.hazelcast.simulator.protocol.operation.RcDownloadOperation;
import com.hazelcast.simulator.protocol.operation.RcTestRunOperation;
import com.hazelcast.simulator.protocol.operation.RcTestStatusOperation;
import com.hazelcast.simulator.protocol.operation.RcTestStopOperation;
import com.hazelcast.simulator.protocol.operation.RcWorkerKillOperation;
import com.hazelcast.simulator.protocol.operation.RcWorkerScriptOperation;
import com.hazelcast.simulator.protocol.operation.RcWorkerStartOperation;
import com.hazelcast.simulator.protocol.processors.CoordinatorOperationProcessor;
import com.hazelcast.simulator.protocol.registry.AgentData;
import com.hazelcast.simulator.protocol.registry.ComponentRegistry;
import com.hazelcast.simulator.protocol.registry.TestData;
import com.hazelcast.simulator.protocol.registry.WorkerData;
import com.hazelcast.simulator.protocol.registry.WorkerQuery;
import com.hazelcast.simulator.utils.Bash;
import com.hazelcast.simulator.utils.CommandLineExitException;
import com.hazelcast.simulator.utils.CommonUtils;
import com.hazelcast.simulator.utils.ThreadSpawner;
import com.hazelcast.simulator.worker.Promise;
import org.apache.log4j.Logger;

import java.io.File;
import java.util.HashMap;
import java.util.List;
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
import static com.hazelcast.simulator.utils.CommonUtils.sleepSeconds;
import static com.hazelcast.simulator.utils.FileUtils.ensureNewDirectory;
import static com.hazelcast.simulator.utils.FileUtils.getUserDir;
import static com.hazelcast.simulator.utils.HazelcastUtils.initClientHzConfig;
import static com.hazelcast.simulator.utils.HazelcastUtils.initMemberHzConfig;
import static java.lang.String.format;
import static java.util.Collections.singleton;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

@SuppressWarnings({"checkstyle:classdataabstractioncoupling", "checkstyle:classfanoutcomplexity"})
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

    private final TestPhase lastTestPhaseToSync;

    private RemoteClient remoteClient;
    private CoordinatorConnector coordinatorConnector;

    private CountDownLatch remoteModeInitialized = new CountDownLatch(1);

    Coordinator(ComponentRegistry componentRegistry, CoordinatorParameters coordinatorParameters) {

        this.outputDirectory = ensureNewDirectory(new File(getUserDir(), coordinatorParameters.getSessionId()));
        this.componentRegistry = componentRegistry;
        this.coordinatorParameters = coordinatorParameters;
        this.failureCollector = new FailureCollector(outputDirectory, componentRegistry);
        this.simulatorProperties = coordinatorParameters.getSimulatorProperties();
        this.bash = new Bash(simulatorProperties);
        this.lastTestPhaseToSync = coordinatorParameters.getLastTestPhaseToSync();
    }

    private void logConfiguration(DeploymentPlan deploymentPlan) {
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

    void run(DeploymentPlan deploymentPlan, TestSuite testSuite) {
        logConfiguration(deploymentPlan);

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

    void startRemoteMode() {
        echoLocal("Coordinator remote mode starting...");

        checkInstallation(bash, simulatorProperties, componentRegistry);

        startAgents(LOGGER, bash, simulatorProperties, componentRegistry);
        startCoordinatorConnector();
        startRemoteClient();

        new InstallVendorTask(
                simulatorProperties,
                componentRegistry.getAgentIps(),
                singleton(simulatorProperties.getVersionSpec()),
                coordinatorParameters.getSessionId()).run();


        echoLocal("Total number of agents: %s", componentRegistry.agentCount());
        echoLocal("Output directory: " + outputDirectory.getAbsolutePath());
        int performanceIntervalSeconds = coordinatorParameters.getPerformanceMonitorIntervalSeconds();
        if (performanceIntervalSeconds > 0) {
            echoLocal("Performance monitor enabled (%d seconds)", performanceIntervalSeconds);
        } else {
            echoLocal("Performance monitor disabled");
        }

        remoteModeInitialized.countDown();

        echoLocal("Coordinator remote mode started...");
    }

    private void awaitInteractiveModeInitialized() throws Exception {
        if (!remoteModeInitialized.await(INTERACTIVE_MODE_INITIALIZE_TIMEOUT_MINUTES, MINUTES)) {
            throw new TimeoutException("Coordinator interactive mode failed to complete");
        }
    }

    private void startRemoteClient() {
        LOGGER.info("Remote client starting....");
        int workerPingIntervalMillis = (int) SECONDS.toMillis(simulatorProperties.getWorkerPingIntervalSeconds());

        remoteClient = new RemoteClient(coordinatorConnector, componentRegistry, workerPingIntervalMillis);
        remoteClient.invokeOnAllAgents(new InitSessionOperation(coordinatorParameters.getSessionId()));
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


    public void download(RcDownloadOperation operation) throws Exception {
        awaitInteractiveModeInitialized();

        LOGGER.info("Downloading ....");

        new DownloadTask(
                coordinatorParameters.getSessionId(),
                simulatorProperties,
                outputDirectory,
                componentRegistry).run();

        LOGGER.info("Downloading complete!");
    }

    public void exit() {
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
                    CommonUtils.exit(0);
                } catch (Exception e) {
                    LOGGER.warn("Failed to shutdown", e);
                }
            }
        }).start();
    }

    public void install(String versionSpec) throws Exception {
        awaitInteractiveModeInitialized();

        LOGGER.info("Installing versionSpec [" + versionSpec + "] on " + componentRegistry.getAgents().size() + " agents ....");
        new InstallVendorTask(
                simulatorProperties,
                componentRegistry.getAgentIps(),
                singleton(versionSpec),
                coordinatorParameters.getSessionId()).run();
        LOGGER.info("Install successful!");
    }

    public String printLayout() throws Exception {
        awaitInteractiveModeInitialized();

        return componentRegistry.printLayout();
    }

    public String testStatus(RcTestStatusOperation op) throws Exception {
        awaitInteractiveModeInitialized();

        TestData test = componentRegistry.getTestByAddress(SimulatorAddress.fromString(op.getTestId()));
        if (test == null) {
            return "null";
        }

        return test.getStatusString();
    }

    public void testStop(RcTestStopOperation op) throws Exception {
        awaitInteractiveModeInitialized();

        TestData data = componentRegistry.getTestByAddress(SimulatorAddress.fromString(op.getTestId()));
        if (data == null) {
            throw new IllegalStateException(format("no test with id [%s] found", op.getTestId()));
        }

        data.setStopRequested(true);
    }

    public void testRun(RcTestRunOperation op, Promise promise) throws Exception {
        awaitInteractiveModeInitialized();

        LOGGER.info("Run starting...");
        final RunTestSuiteTask runTestSuiteTask = new RunTestSuiteTask(op.getTestSuite(),
                coordinatorParameters,
                componentRegistry,
                failureCollector,
                testPhaseListeners,
                remoteClient,
                performanceStatsCollector);

        if (op.isAsync()) {
            if (op.getTestSuite().size() > 1) {
                throw new IllegalArgumentException("1 test in testsuite allowed");
            }

            new Thread(new Runnable() {
                @Override
                public void run() {
                    runTestSuiteTask.run();
                }
            }).start();

            for (; ; ) {
                sleepSeconds(1);
                for (TestData testData : componentRegistry.getTests()) {
                    if (testData.getTestSuite() == op.getTestSuite()) {
                        promise.answer(ResponseType.SUCCESS, testData.getAddress().toString());
                    }
                }
            }
        } else {
            boolean success = runTestSuiteTask.run();
            LOGGER.info("Run complete!");

            if (success) {
                promise.answer(ResponseType.SUCCESS);
            } else {
                promise.answer(ResponseType.EXCEPTION_DURING_OPERATION_EXECUTION, "Run completed with failures!");
            }
        }
    }

    public String workerStart(RcWorkerStartOperation op) throws Exception {
        awaitInteractiveModeInitialized();

        WorkerType workerType = new WorkerType(op.getWorkerType());

        LOGGER.info("Starting " + op.getCount() + " [" + workerType + "] workers....");

        Map<String, String> environment = new HashMap<String, String>();
        environment.putAll(simulatorProperties.asMap());
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

        WorkerParameters workerParameters = new WorkerParameters()
                .setVersionSpec(versionSpec)
                .setWorkerStartupTimeout(simulatorProperties.getAsInteger("WORKER_STARTUP_TIMEOUT_SECONDS"))
                .setWorkerScript(loadWorkerScript(workerType, simulatorProperties.get("VENDOR")))
                .setEnvironment(environment);

        SimulatorAddress agent = op.getAgentAddress() == null ? null : SimulatorAddress.fromString(op.getAgentAddress());
        DeploymentPlan deploymentPlan = createDeploymentPlan(
                componentRegistry, workerParameters, workerType, op.getCount(), agent);

        List<WorkerData> workers = new StartWorkersTask(
                deploymentPlan.getWorkerDeployment(),
                remoteClient,
                componentRegistry,
                coordinatorParameters.getWorkerVmStartupDelayMs()
        ).run();

        LOGGER.info("Workers started!");

        return WorkerData.toAddressString(workers);
    }

    public String workerKill(RcWorkerKillOperation op) throws Exception {
        awaitInteractiveModeInitialized();

        WorkerQuery workerQuery = op.getWorkerQuery();

        LOGGER.info(format("Killing %s worker with versionSpec [%s] and workerType [%s]...",
                workerQuery.getMaxCount(), workerQuery.getVersionSpec(), workerQuery.getWorkerType()));

        List<WorkerData> result = new KillWorkersTask(
                componentRegistry, coordinatorConnector, op.getCommand(), workerQuery).run();

        LOGGER.info("\n" + componentRegistry.printLayout());

        LOGGER.info(format("Killing %s worker with versionSpec [%s] and workerType [%s] completed!",
                workerQuery.getMaxCount(), workerQuery.getVersionSpec(), workerQuery.getWorkerType()));

        return WorkerData.toAddressString(result);
    }

    public void workerScript(RcWorkerScriptOperation operation) throws Exception {
        awaitInteractiveModeInitialized();

        List<WorkerData> workers = operation.getWorkerQuery().execute(componentRegistry.getWorkers());

        LOGGER.info(format("Script [%s] on %s workers ...", operation.getCommand(), workers.size()));

        for (WorkerData worker : workers) {
            coordinatorConnector.invoke(worker.getAddress(), new ExecuteScriptOperation(operation.getCommand()));
            LOGGER.info("Script send to worker [" + worker.getAddress() + "]");
        }

        LOGGER.info(format("Script [%s] on %s workers completed!", operation.getCommand(), workers.size()));
    }

}
