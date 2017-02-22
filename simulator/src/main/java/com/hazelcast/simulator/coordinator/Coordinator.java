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

import com.hazelcast.simulator.agent.workerprocess.WorkerProcessSettings;
import com.hazelcast.simulator.common.SimulatorProperties;
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
import com.hazelcast.simulator.utils.CommonUtils;
import com.hazelcast.simulator.utils.ThreadSpawner;
import com.hazelcast.simulator.worker.Promise;
import org.apache.log4j.Logger;

import java.io.Closeable;
import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
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
import static com.hazelcast.simulator.utils.TagUtils.matches;
import static java.lang.Runtime.getRuntime;
import static java.lang.String.format;
import static java.util.Collections.singleton;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

@SuppressWarnings({"checkstyle:classdataabstractioncoupling", "checkstyle:classfanoutcomplexity"})
public class Coordinator implements Closeable {

    private static final int INITIALIZED_TIMEOUT_MINUTES = 5;
    private static final Logger LOGGER = Logger.getLogger(Coordinator.class);

    private final CountDownLatch initialized = new CountDownLatch(1);
    private final TestPhaseListeners testPhaseListeners = new TestPhaseListeners();
    private final PerformanceStatsCollector performanceStatsCollector = new PerformanceStatsCollector();

    private final ComponentRegistry componentRegistry;
    private final CoordinatorParameters parameters;
    private final File outputDirectory;
    private final FailureCollector failureCollector;
    private final SimulatorProperties simulatorProperties;
    private final Bash bash;
    private final int testCompletionTimeoutSeconds;

    private CoordinatorConnector connector;
    private RemoteClient client;

    Coordinator(ComponentRegistry componentRegistry, CoordinatorParameters parameters) {
        this.componentRegistry = componentRegistry;
        this.parameters = parameters;
        this.outputDirectory = ensureNewDirectory(new File(getUserDir(), parameters.getSessionId()));
        this.failureCollector = new FailureCollector(outputDirectory, componentRegistry);
        this.simulatorProperties = parameters.getSimulatorProperties();
        this.bash = new Bash(simulatorProperties);
        this.testCompletionTimeoutSeconds = simulatorProperties.getTestCompletionTimeoutSeconds();
    }

    FailureCollector getFailureCollector() {
        return failureCollector;
    }

    void start() {
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
        if (parameters.skipShutdownHook()) {
            return;
        }

        getRuntime().addShutdownHook(new Thread() {
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

    @Override
    public void close() {
        stopTests();

        if (client != null) {
            new TerminateWorkersTask(simulatorProperties, componentRegistry, client).run();
        }

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

    private void stopTests() {
        Collection<TestData> tests = componentRegistry.getTests();
        for (TestData testData : tests) {
            testData.setStopRequested(true);
        }

        for (int i = 0; i < testCompletionTimeoutSeconds; i++) {
            Iterator<TestData> it = tests.iterator();
            while (it.hasNext()) {
                TestData test = it.next();

                if (test.isCompleted()) {
                    it.remove();
                }
            }

            sleepSeconds(1);

            if (tests.isEmpty()) {
                return;
            }
        }

        LOGGER.info("The following tests failed to complete: " + tests);
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

        LOGGER.info("Remote client starting...");
        int workerPingIntervalMillis = (int) SECONDS.toMillis(simulatorProperties.getWorkerPingIntervalSeconds());

        client = new RemoteClient(connector, componentRegistry, workerPingIntervalMillis);
        client.invokeOnAllAgents(new InitSessionOperation(parameters.getSessionId()));
        LOGGER.info("Remote client started successfully!");
    }

    public void download() throws Exception {
        awaitInitialized();

        LOGGER.info("Downloading...");

        new ArtifactDownloadTask(
                parameters.getSessionId(),
                simulatorProperties,
                outputDirectory,
                componentRegistry).run();

        LOGGER.info("Downloading complete!");
    }

    public void exit() throws Exception {
        awaitInitialized();

        LOGGER.info("Shutting down...");

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

        LOGGER.info("Installing versionSpec [" + versionSpec + "] on " + componentRegistry.getAgents().size() + " agents...");
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

    public void testRun(RcTestRunOperation op, Promise promise) throws Exception {
        awaitInitialized();

        LOGGER.info("Run starting...");
        final RunTestSuiteTask runTestSuiteTask = createRunTestSuiteTask(op.getTestSuite());

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

    public String testStop(RcTestStopOperation op) throws Exception {
        awaitInitialized();

        LOGGER.info(format("Test [%s] stopping...", op.getTestId()));

        TestData test = componentRegistry.getTestByAddress(SimulatorAddress.fromString(op.getTestId()));
        if (test == null) {
            throw new IllegalStateException(format("no test with id [%s] found", op.getTestId()));
        }

        for (int i = 0; i < testCompletionTimeoutSeconds; i++) {
            test.setStopRequested(true);

            sleepSeconds(1);

            if (test.isCompleted()) {
                return test.getStatusString();
            }
        }

        throw new Exception("Test failed to stop within " + testCompletionTimeoutSeconds
                + " seconds, current status: " + test.getStatusString());
    }

    public String testStatus(RcTestStatusOperation op) throws Exception {
        awaitInitialized();

        TestData test = componentRegistry.getTestByAddress(SimulatorAddress.fromString(op.getTestId()));
        if (test == null) {
            return "null";
        }

        return test.getStatusString();
    }

    public String workerStart(RcWorkerStartOperation op) throws Exception {
        awaitInitialized();

        WorkerType workerType = new WorkerType(op.getWorkerType());

        LOGGER.info("Starting " + op.getCount() + " [" + workerType + "] workers...");

        String versionSpec = op.getVersionSpec() == null
                ? simulatorProperties.getVersionSpec()
                : op.getVersionSpec();

        String vendor = simulatorProperties.get("VENDOR");
        WorkerParameters workerParameters = new WorkerParameters()
                .setVersionSpec(versionSpec)
                .addEnvironment("WORKER_TYPE", workerType.name())
                .addEnvironment("VENDOR", vendor)
                .setWorkerStartupTimeout(simulatorProperties.getWorkerStartupTimeoutSeconds())
                .setWorkerScript(loadWorkerScript(workerType, vendor));

        List<SimulatorAddress> agents = findAgents(op);
        LOGGER.info("Suitable agents: " + agents);
        if (agents.isEmpty()) {
            throw new IllegalStateException("No suitable agents found");
        }

        DeploymentPlan deploymentPlan = createDeploymentPlan(
                componentRegistry, workerParameters, workerType, op.getCount(), agents);

        Map<SimulatorAddress, List<WorkerProcessSettings>> workerDeployment
                = deploymentPlan.getWorkerDeployment();

        // we need to fix the environment of the worker so it contains the appropriate tags/configuration
        // this can only be done after the deployment plan is made.
        for (Map.Entry<SimulatorAddress, List<WorkerProcessSettings>> entry : workerDeployment.entrySet()) {
            SimulatorAddress agentAddress = entry.getKey();
            AgentData agentData = componentRegistry.getAgent(agentAddress);

            for (WorkerProcessSettings workerProcessSettings : entry.getValue()) {
                Map<String, String> env = workerProcessSettings.getEnvironment();
                env.putAll(simulatorProperties.asMap());
                env.put("HAZELCAST_CONFIG", loadConfig(op, workerType, agentData.getTags()));
                env.put("AUTOCREATE_HAZELCAST_INSTANCE", "true");
                env.put("LOG4j_CONFIG", loadLog4jConfig());
                env.put("JVM_OPTIONS", op.getVmOptions());
                env.put("WORKER_PERFORMANCE_MONITOR_INTERVAL_SECONDS",
                        "" + parameters.getPerformanceMonitorIntervalSeconds());
                // first we add the agent tags, since each worker inherits the tags of the agent
                env.putAll(agentData.getTags());
                // and on top we add the specific tags for the worker
                env.putAll(op.getTags());
            }
        }

        List<WorkerData> workers = createStartWorkersTask(workerDeployment, op.getTags()).run();

        LOGGER.info("Workers started!");

        return WorkerData.toAddressString(workers);
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

    StartWorkersTask createStartWorkersTask(Map<SimulatorAddress, List<WorkerProcessSettings>> deploymentPlan,
                                            Map<String, String> workerTags) {
        return new StartWorkersTask(
                deploymentPlan,
                workerTags,
                client,
                componentRegistry,
                parameters.getWorkerVmStartupDelayMs());
    }

    RunTestSuiteTask createRunTestSuiteTask(TestSuite testSuite) {
        return new RunTestSuiteTask(testSuite,
                parameters,
                componentRegistry,
                failureCollector,
                testPhaseListeners,
                client,
                performanceStatsCollector);
    }

    private List<SimulatorAddress> findAgents(RcWorkerStartOperation op) {
        List<AgentData> agents = new ArrayList<AgentData>(componentRegistry.getAgents());
        List<SimulatorAddress> result = new ArrayList<SimulatorAddress>();
        for (AgentData agent : agents) {
            List<String> expectedAgentAddresses = op.getAgentAddresses();

            if (expectedAgentAddresses != null) {
                if (!expectedAgentAddresses.contains(agent.getAddress().toString())) {
                    continue;
                }
            }

            Map<String, String> expectedAgentTags = op.getAgentTags();
            if (expectedAgentTags != null) {
                if (!matches(op.getAgentTags(), agent.getTags())) {
                    continue;
                }
            }

            result.add(agent.getAddress());
        }
        return result;
    }

    private String loadConfig(RcWorkerStartOperation op, WorkerType workerType, Map<String, String> agentTags) {
        Map<String, String> env = new HashMap<String, String>(simulatorProperties.asMap());
        env.putAll(agentTags);
        env.putAll(op.getTags());

        String config;
        if (WorkerType.MEMBER.equals(workerType)) {
            config = initMemberHzConfig(
                    op.getHzConfig() == null ? loadMemberHzConfig() : op.getHzConfig(),
                    componentRegistry,
                    parameters.getLicenseKey(),
                    env, false);
        } else if (WorkerType.LITE_MEMBER.equals(workerType)) {
            config = initMemberHzConfig(
                    op.getHzConfig() == null ? loadMemberHzConfig() : op.getHzConfig(),
                    componentRegistry,
                    parameters.getLicenseKey(),
                    env, true);
        } else if (WorkerType.JAVA_CLIENT.equals(workerType)) {
            config = initClientHzConfig(
                    op.getHzConfig() == null ? loadClientHzConfig() : op.getHzConfig(),
                    componentRegistry,
                    env,
                    parameters.getLicenseKey());
        } else {
            throw new IllegalStateException("Unrecognized workerType [" + workerType + "]");
        }
        return config;
    }

    private void awaitInitialized() throws Exception {
        if (!initialized.await(INITIALIZED_TIMEOUT_MINUTES, MINUTES)) {
            throw new TimeoutException("Coordinator remote mode failed to initialize");
        }
    }

    private static void echoLocal(String message, Object... args) {
        String log = message == null ? "null" : format(message, args);
        LOGGER.info(log);
    }
}
