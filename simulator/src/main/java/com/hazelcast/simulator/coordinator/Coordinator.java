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

import com.hazelcast.simulator.agent.operations.InitSessionOperation;
import com.hazelcast.simulator.agent.workerprocess.WorkerProcessSettings;
import com.hazelcast.simulator.common.SimulatorProperties;
import com.hazelcast.simulator.common.WorkerType;
import com.hazelcast.simulator.coordinator.operations.RcTestRunOperation;
import com.hazelcast.simulator.coordinator.operations.RcTestStatusOperation;
import com.hazelcast.simulator.coordinator.operations.RcTestStopOperation;
import com.hazelcast.simulator.coordinator.operations.RcWorkerKillOperation;
import com.hazelcast.simulator.coordinator.operations.RcWorkerScriptOperation;
import com.hazelcast.simulator.coordinator.operations.RcWorkerStartOperation;
import com.hazelcast.simulator.coordinator.registry.AgentData;
import com.hazelcast.simulator.coordinator.registry.ComponentRegistry;
import com.hazelcast.simulator.coordinator.registry.TestData;
import com.hazelcast.simulator.coordinator.registry.WorkerData;
import com.hazelcast.simulator.coordinator.registry.WorkerQuery;
import com.hazelcast.simulator.coordinator.tasks.DownloadTask;
import com.hazelcast.simulator.coordinator.tasks.InstallVendorTask;
import com.hazelcast.simulator.coordinator.tasks.KillWorkersTask;
import com.hazelcast.simulator.coordinator.tasks.RunTestSuiteTask;
import com.hazelcast.simulator.coordinator.tasks.StartWorkersTask;
import com.hazelcast.simulator.coordinator.tasks.TerminateWorkersTask;
import com.hazelcast.simulator.protocol.CoordinatorClient;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.utils.CommonUtils;
import com.hazelcast.simulator.worker.operations.ExecuteScriptOperation;
import org.apache.log4j.Logger;

import java.io.Closeable;
import java.io.File;
import java.rmi.AlreadyBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import static com.hazelcast.simulator.coordinator.AgentUtils.startAgents;
import static com.hazelcast.simulator.coordinator.AgentUtils.stopAgents;
import static com.hazelcast.simulator.coordinator.CoordinatorCli.loadClientHzConfig;
import static com.hazelcast.simulator.coordinator.CoordinatorCli.loadLog4jConfig;
import static com.hazelcast.simulator.coordinator.CoordinatorCli.loadMemberHzConfig;
import static com.hazelcast.simulator.coordinator.CoordinatorCli.loadWorkerScript;
import static com.hazelcast.simulator.coordinator.DeploymentPlan.createDeploymentPlan;
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

@SuppressWarnings({"checkstyle:classdataabstractioncoupling", "checkstyle:classfanoutcomplexity"})
public class Coordinator implements Closeable {

    private static final int INITIALIZED_TIMEOUT_MINUTES = 5;
    private static final Logger LOGGER = Logger.getLogger(Coordinator.class);

    private final CountDownLatch initialized = new CountDownLatch(1);
    private final PerformanceStatsCollector performanceStatsCollector = new PerformanceStatsCollector();

    private final ComponentRegistry componentRegistry;
    private final CoordinatorParameters parameters;
    private final File outputDirectory;
    private final FailureCollector failureCollector;
    private final SimulatorProperties simulatorProperties;
    private final int testCompletionTimeoutSeconds;
    private final CoordinatorClient client;

    Coordinator(ComponentRegistry componentRegistry, CoordinatorParameters parameters) {
        this.componentRegistry = componentRegistry;
        this.parameters = parameters;
        this.outputDirectory = ensureNewDirectory(new File(getUserDir(), parameters.getSessionId()));
        this.failureCollector = new FailureCollector(outputDirectory, componentRegistry);
        this.simulatorProperties = parameters.getSimulatorProperties();
        this.testCompletionTimeoutSeconds = simulatorProperties.getTestCompletionTimeoutSeconds();

        this.client = new CoordinatorClient()
                .setAgentBrokerPort(simulatorProperties.getAgentPort())
                .setOperationProcessor(new CoordinatorOperationProcessor(failureCollector, performanceStatsCollector))
                .setFailureCollector(failureCollector);
    }

    FailureCollector getFailureCollector() {
        return failureCollector;
    }

    void start() throws Exception {
        client.start();

        registerShutdownHook();

        logConfiguration();

        echoLocal("Coordinator starting...");

        startAgents(simulatorProperties, componentRegistry);

        startClient();

        new InstallVendorTask(
                simulatorProperties,
                componentRegistry.getAgentIps(),
                singleton(simulatorProperties.getVersionSpec()),
                parameters.getSessionId()).run();

        initOptionalRemote();

        initialized.countDown();

        echoLocal("Coordinator started...");
    }

    private void initOptionalRemote() throws RemoteException, AlreadyBoundException {
        int remotePort = simulatorProperties.getCoordinatorPort();
        if (remotePort != 0) {
            Registry registry = LocateRegistry.createRegistry(remotePort);
            CoordinatorRemote r = new CoordinatorRemoteImpl(this);
            Remote stub = UnicastRemoteObject.exportObject(r, 0);

            // Bind the remote object's stub in the registry
            registry.bind("CoordinatorRemote", stub);
        }
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
            echoLocal("Performance monitor enabled (%d seconds interval)", performanceIntervalSeconds);
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

        new TerminateWorkersTask(simulatorProperties, componentRegistry, client).run();

        client.close();

        stopAgents(simulatorProperties, componentRegistry);

        if (!parameters.skipDownload()) {
            new DownloadTask(componentRegistry.getAgentIps(),
                    simulatorProperties.asMap(),
                    outputDirectory.getParentFile(),
                    parameters.getSessionId()).run();
        }

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

    private void startClient() throws Exception {
        // todo: should be async to speed things up
        for (AgentData agentData : componentRegistry.getAgents()) {
            client.connectToAgentBroker(agentData.getAddress(), agentData.getPublicAddress());
        }

        LOGGER.info("Remote client started successfully!");

        client.invokeAll(componentRegistry.getAgents(), new InitSessionOperation(parameters.getSessionId()), MINUTES.toMillis(1));
    }

    public void download() throws Exception {
        awaitInitialized();

        LOGGER.info("Downloading...");

        new DownloadTask(componentRegistry.getAgentIps(),
                simulatorProperties.asMap(),
                outputDirectory.getParentFile(),
                parameters.getSessionId()).run();

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

    public String testRun(RcTestRunOperation op) throws Exception {
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
                        return testData.getAddress().toString();
                    }
                }
            }
        } else {
            boolean success = runTestSuiteTask.run();
            LOGGER.info("Run complete!");
            return success ? null : "Run completed with failures!";
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
                componentRegistry, client, op.getCommand(), workerQuery).run();

        LOGGER.info("\n" + componentRegistry.printLayout());

        LOGGER.info(format("Killing %s complete", workerQuery));

        return WorkerData.toAddressString(result);
    }

    public String workerScript(RcWorkerScriptOperation operation) throws Exception {
        awaitInitialized();

        List<WorkerData> workers = operation.getWorkerQuery().execute(componentRegistry.getWorkers());

        LOGGER.info(format("Script [%s] on %s workers ...", operation.getCommand(), workers.size()));

        Map<WorkerData, Future<String>> futures = new HashMap<WorkerData, Future<String>>();
        for (WorkerData worker : workers) {
            Future<String> f = client.submit(worker.getAddress(),
                    new ExecuteScriptOperation(operation.getCommand(), operation.isFireAndForget()));
            futures.put(worker, f);
            LOGGER.info("Script send to worker [" + worker.getAddress() + "]");
        }

        if (operation.isFireAndForget()) {
            return null;
        }

        StringBuilder sb = new StringBuilder();
        for (Map.Entry<WorkerData, Future<String>> entry : futures.entrySet()) {
            WorkerData worker = entry.getKey();
            String result = entry.getValue().get();
            sb.append(worker.getAddress()).append("=").append(result).append("\n");
        }

        LOGGER.info(format("Script [%s] on %s workers completed!", operation.getCommand(), workers.size()));
        return sb.toString();
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
