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
import com.hazelcast.simulator.coordinator.tasks.ArtifactDownloadTask;
import com.hazelcast.simulator.coordinator.tasks.InstallVendorTask;
import com.hazelcast.simulator.coordinator.tasks.KillWorkersTask;
import com.hazelcast.simulator.coordinator.tasks.RunTestSuiteTask;
import com.hazelcast.simulator.coordinator.tasks.StartWorkersTask;
import com.hazelcast.simulator.coordinator.tasks.TerminateWorkersTask;
import com.hazelcast.simulator.protocol.connector.CoordinatorConnector;
import com.hazelcast.simulator.protocol.core.Response;
import com.hazelcast.simulator.protocol.core.ResponseFuture;
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
import com.hazelcast.simulator.protocol.registry.TestData.CompletedStatus;
import com.hazelcast.simulator.protocol.registry.WorkerData;
import com.hazelcast.simulator.protocol.registry.WorkerQuery;
import com.hazelcast.simulator.utils.Bash;
import com.hazelcast.simulator.utils.CommonUtils;
import com.hazelcast.simulator.utils.ThreadSpawner;
import com.hazelcast.simulator.worker.Promise;
import org.apache.log4j.Logger;

import java.io.Closeable;
import java.io.File;
import java.util.ArrayList;
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
import static com.hazelcast.simulator.protocol.core.ResponseType.EXCEPTION_DURING_OPERATION_EXECUTION;
import static com.hazelcast.simulator.protocol.core.ResponseType.SUCCESS;
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
public class Coordinator implements Closeable {

    private static final Logger LOGGER = Logger.getLogger(Coordinator.class);
    private static final int INITIALIZED_TIMEOUT_MINUTES = 5;

    ComponentRegistry componentRegistry;
    RemoteClient client;
    final CoordinatorParameters parameters;
    final FailureCollector failureCollector;
    final TestPhaseListeners testPhaseListeners = new TestPhaseListeners();
    final PerformanceStatsCollector performanceStatsCollector = new PerformanceStatsCollector();
    final File outputDirectory;
    // for testing purposes; we don't want to have a shutdownhook registered.
    boolean skipShutdownHook;

    private final SimulatorProperties simulatorProperties;
    private final Bash bash;
    private final TestPhase lastTestPhaseToSync;
    private final CountDownLatch initialized = new CountDownLatch(1);
    private CoordinatorConnector connector;

    Coordinator(ComponentRegistry componentRegistry, CoordinatorParameters parameters) {
        this.outputDirectory = ensureNewDirectory(new File(getUserDir(), parameters.getSessionId()));
        this.componentRegistry = componentRegistry;
        this.parameters = parameters;
        this.failureCollector = new FailureCollector(outputDirectory, componentRegistry);
        this.simulatorProperties = parameters.getSimulatorProperties();
        this.bash = new Bash(simulatorProperties);
        this.lastTestPhaseToSync = parameters.getLastTestPhaseToSync();
    }

    public void start() {
        registerShutdownHook();

        logConfiguration();

        echoLocal("Coordinator starting...");

        checkInstallation(bash, simulatorProperties, componentRegistry);

        startAgents(LOGGER, bash, simulatorProperties, componentRegistry);

        startCoordinatorConnector();

        new InstallVendorTask(
                simulatorProperties,
                componentRegistry.getAgentIps(),
                singleton(simulatorProperties.getVersionSpec()),
                parameters.getSessionId()).run();

        initialized.countDown();

        echoLocal("Coordinator started...");
    }

    private void registerShutdownHook() {
        if (skipShutdownHook) {
            return;
        }

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    close();
                } catch (Throwable t) {
                    LOGGER.fatal(t.getMessage(), t);
                }
            }
        });
    }

    private void logConfiguration() {
        echoLocal("Total number of agents: %s", componentRegistry.agentCount());
        echoLocal("Output directory: " + outputDirectory.getAbsolutePath());

        int performanceIntervalSeconds = parameters.getPerformanceMonitorIntervalSeconds();

        if (performanceIntervalSeconds > 0) {
            echoLocal("Performance monitor enabled (%d seconds)", performanceIntervalSeconds);
        } else {
            echoLocal("Performance monitor disabled");
        }

        if (simulatorProperties.getCoordinatorPort() > 0) {
            echoLocal("Coordinator remote enabled on port " + simulatorProperties.getCoordinatorPort());
        }
    }

    private void awaitInitialized() throws Exception {
        if (!initialized.await(INITIALIZED_TIMEOUT_MINUTES, MINUTES)) {
            throw new TimeoutException("Coordinator remote mode failed to initialize");
        }
    }

    private static String echoLocal(String message, Object... args) {
        String log = message == null ? "null" : format(message, args);
        LOGGER.info(log);
        return log;
    }


    public void download(RcDownloadOperation operation) throws Exception {
        awaitInitialized();

        LOGGER.info("Downloading ....");

        new ArtifactDownloadTask(
                parameters.getSessionId(),
                simulatorProperties,
                outputDirectory,
                componentRegistry).run();

        LOGGER.info("Downloading complete!");
    }

    public void exit() throws Exception {
        awaitInitialized();

        LOGGER.info("Shutting down....");

        new Thread(new Runnable() {
            private static final int DELAY = 5000;

            @Override
            public void run() {
                try {
                    Thread.sleep(DELAY);
                    CommonUtils.exit(0);
                } catch (Exception e) {
                    LOGGER.warn("Failed to shutdown", e);
                }
            }
        }).start();
    }

    public void install(String versionSpec) throws Exception {
        awaitInitialized();

        LOGGER.info("Installing versionSpec [" + versionSpec + "] on " + componentRegistry.getAgents().size() + " agents ....");
        new InstallVendorTask(
                simulatorProperties,
                componentRegistry.getAgentIps(),
                singleton(versionSpec),
                parameters.getSessionId()).run();
        LOGGER.info("Install successful!");
    }

    public String printLayout() throws Exception {
        awaitInitialized();

        return componentRegistry.printLayout();
    }

    public String testStatus(RcTestStatusOperation op) throws Exception {
        awaitInitialized();

        TestData test = componentRegistry.getTestByAddress(SimulatorAddress.fromString(op.getTestId()));
        if (test == null) {
            return "null";
        }

        return test.getStatusString();
    }

    public String testStop(RcTestStopOperation op) throws Exception {
        awaitInitialized();

        LOGGER.info(format("Test [%s] stopping...", op.getTestId()));

        TestData data = componentRegistry.getTestByAddress(SimulatorAddress.fromString(op.getTestId()));
        if (data == null) {
            throw new IllegalStateException(format("no test with id [%s] found", op.getTestId()));
        }

        for (; ; ) {
            data.setStopRequested(true);
            sleepSeconds(1);
            if (data.getCompletedStatus() == CompletedStatus.SUCCESS || data.getCompletedStatus() == CompletedStatus.FAILED) {
                return data.getStatusString();
            }
        }
    }

    public void testRun(RcTestRunOperation op, Promise promise) throws Exception {
        awaitInitialized();

        LOGGER.info("Run starting...");
        final RunTestSuiteTask runTestSuiteTask = new RunTestSuiteTask(op.getTestSuite(),
                parameters,
                componentRegistry,
                failureCollector,
                testPhaseListeners,
                client,
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
                        promise.answer(SUCCESS, testData.getAddress().toString());
                        return;
                    }
                }
            }
        } else {
            boolean success = runTestSuiteTask.run();
            LOGGER.info("Run complete!");

            if (success) {
                promise.answer(SUCCESS);
            } else {
                promise.answer(EXCEPTION_DURING_OPERATION_EXECUTION, "Run completed with failures!");
            }
        }
    }

    public String workerStart(RcWorkerStartOperation op) throws Exception {
        awaitInitialized();

        WorkerType workerType = new WorkerType(op.getWorkerType());

        LOGGER.info("Starting " + op.getCount() + " [" + workerType + "] workers....");

        Map<String, String> environment = new HashMap<String, String>();
        environment.putAll(simulatorProperties.asMap());
        environment.put("AUTOCREATE_HAZELCAST_INSTANCE", "true");
        environment.put("LOG4j_CONFIG", loadLog4jConfig());
        environment.put("JVM_OPTIONS", op.getVmOptions());
        environment.put("WORKER_PERFORMANCE_MONITOR_INTERVAL_SECONDS",
                "" + parameters.getPerformanceMonitorIntervalSeconds());
        environment.put("HAZELCAST_CONFIG", loadConfig(op, workerType));

        String versionSpec = op.getVersionSpec() == null
                ? simulatorProperties.getVersionSpec()
                : op.getVersionSpec();

        WorkerParameters workerParameters = new WorkerParameters()
                .setVersionSpec(versionSpec)
                .setWorkerStartupTimeout(simulatorProperties.getAsInteger("WORKER_STARTUP_TIMEOUT_SECONDS"))
                .setWorkerScript(loadWorkerScript(workerType, simulatorProperties.get("VENDOR")))
                .setEnvironment(environment);

        List<SimulatorAddress> agents = op.getAgentAddress() == null ? null : SimulatorAddress.fromString(op.getAgentAddress());

        DeploymentPlan deploymentPlan = createDeploymentPlan(
                componentRegistry, workerParameters, workerType, op.getCount(), agents);

        List<WorkerData> workers = new StartWorkersTask(
                deploymentPlan.getWorkerDeployment(),
                client,
                componentRegistry,
                parameters.getWorkerVmStartupDelayMs()).run();

        LOGGER.info("Workers started!");

        return WorkerData.toAddressString(workers);
    }

    private String loadConfig(RcWorkerStartOperation op, WorkerType workerType) {
        String config;
        if (WorkerType.MEMBER.equals(workerType)) {
            config = initMemberHzConfig(
                    op.getHzConfig() == null ? loadMemberHzConfig() : op.getHzConfig(),
                    componentRegistry,
                    parameters.getLicenseKey(),
                    simulatorProperties, false);
        } else if (WorkerType.LITE_MEMBER.equals(workerType)) {
            config = initMemberHzConfig(
                    op.getHzConfig() == null ? loadMemberHzConfig() : op.getHzConfig(),
                    componentRegistry,
                    parameters.getLicenseKey(),
                    simulatorProperties, true);
        } else if (WorkerType.JAVA_CLIENT.equals(workerType)) {
            config = initClientHzConfig(
                    op.getHzConfig() == null ? loadClientHzConfig() : op.getHzConfig(),
                    componentRegistry,
                    simulatorProperties,
                    parameters.getLicenseKey());
        } else {
            throw new IllegalStateException("Unrecognized workerType [" + workerType + "]");
        }
        return config;
    }

    public String workerKill(RcWorkerKillOperation op) throws Exception {
        awaitInitialized();

        WorkerQuery workerQuery = op.getWorkerQuery();

        LOGGER.info(format("Killing %s...", workerQuery));

        List<WorkerData> result = new KillWorkersTask(
                componentRegistry, connector, op.getCommand(), workerQuery).run();

        LOGGER.info("\n" + componentRegistry.printLayout());

        LOGGER.info(format("Killing %s complete", workerQuery));

        return WorkerData.toAddressString(result);
    }

    public void workerScript(RcWorkerScriptOperation operation, Promise promise) throws Exception {
        awaitInitialized();

        List<WorkerData> workers = operation.getWorkerQuery().execute(componentRegistry.getWorkers());

        LOGGER.info(format("Script [%s] on %s workers ...", operation.getCommand(), workers.size()));

        List<ResponseFuture> futures = new ArrayList<ResponseFuture>();
        for (WorkerData worker : workers) {
            ResponseFuture f = connector.invokeAsync(worker.getAddress(),
                    new ExecuteScriptOperation(operation.getCommand(), operation.isFireAndForget()));
            futures.add(f);
            LOGGER.info("Script send to worker [" + worker.getAddress() + "]");
        }

        if (operation.isFireAndForget()) {
            promise.answer(SUCCESS);
            return;
        }

        StringBuilder sb = new StringBuilder();
        for (ResponseFuture future : futures) {
            Response response = future.get();
            Response.Part errorPart = response.getFirstErrorPart();
            if (errorPart != null) {
                promise.answer(errorPart.getType(), errorPart.getPayload());
                return;
            }

            for (Map.Entry<SimulatorAddress, Response.Part> entry : response.getParts()) {
                sb.append(entry.getKey()).append("=").append(entry.getValue().getPayload()).append("\n");
            }
        }

        LOGGER.info(format("Script [%s] on %s workers completed!", operation.getCommand(), workers.size()));
        promise.answer(SUCCESS, sb.toString());
    }

    public void close() {
        new TerminateWorkersTask(simulatorProperties, componentRegistry, client).run();

        closeQuietly(client);

        closeQuietly(connector);

        stopAgents(LOGGER, bash, simulatorProperties, componentRegistry);

        if (!parameters.skipDownload()) {
            new ArtifactDownloadTask(
                    parameters.getSessionId(),
                    simulatorProperties,
                    outputDirectory,
                    componentRegistry).run();

            if (parameters.getAfterCompletionFile() != null) {
                echoLocal("Executing after-completion script: " + parameters.getAfterCompletionFile());
                bash.execute(parameters.getAfterCompletionFile() + " " + outputDirectory.getAbsolutePath());
                echoLocal("Finished after-completion script");
            }
        }

        OperationTypeCounter.printStatistics();

        failureCollector.logFailureInfo();
    }

    private void startCoordinatorConnector() {
        CoordinatorOperationProcessor processor = new CoordinatorOperationProcessor(
                this, failureCollector, testPhaseListeners, performanceStatsCollector);

        connector = new CoordinatorConnector(processor, simulatorProperties.getCoordinatorPort());
        connector.start();

        ThreadSpawner spawner = new ThreadSpawner("startCoordinatorConnector", true);
        for (final AgentData agentData : componentRegistry.getAgents()) {
            final int agentPort = simulatorProperties.getAgentPort();
            spawner.spawn(new Runnable() {
                @Override
                public void run() {
                    connector.addAgent(agentData.getAddressIndex(), agentData.getPublicAddress(), agentPort);
                }
            });
        }
        spawner.awaitCompletion();

        LOGGER.info("Remote client starting....");
        int workerPingIntervalMillis = (int) SECONDS.toMillis(simulatorProperties.getWorkerPingIntervalSeconds());

        client = new RemoteClient(connector, componentRegistry, workerPingIntervalMillis);
        client.invokeOnAllAgents(new InitSessionOperation(parameters.getSessionId()));
        LOGGER.info("Remote client started successfully!");
    }
}
