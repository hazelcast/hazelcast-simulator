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

import com.hazelcast.simulator.agent.workerprocess.WorkerParameters;
import com.hazelcast.simulator.common.SimulatorProperties;
import com.hazelcast.simulator.coordinator.operations.RcTestRunOperation;
import com.hazelcast.simulator.coordinator.operations.RcTestStatusOperation;
import com.hazelcast.simulator.coordinator.operations.RcTestStopOperation;
import com.hazelcast.simulator.coordinator.operations.RcWorkerKillOperation;
import com.hazelcast.simulator.coordinator.operations.RcWorkerScriptOperation;
import com.hazelcast.simulator.coordinator.operations.RcWorkerStartOperation;
import com.hazelcast.simulator.coordinator.registry.AgentData;
import com.hazelcast.simulator.coordinator.registry.Registry;
import com.hazelcast.simulator.coordinator.registry.TestData;
import com.hazelcast.simulator.coordinator.registry.WorkerData;
import com.hazelcast.simulator.coordinator.registry.WorkerQuery;
import com.hazelcast.simulator.coordinator.tasks.DownloadTask;
import com.hazelcast.simulator.coordinator.tasks.KillWorkersTask;
import com.hazelcast.simulator.coordinator.tasks.PrepareSessionTask;
import com.hazelcast.simulator.coordinator.tasks.RunTestSuiteTask;
import com.hazelcast.simulator.coordinator.tasks.StartWorkersTask;
import com.hazelcast.simulator.coordinator.tasks.TerminateWorkersTask;
import com.hazelcast.simulator.drivers.Driver;
import com.hazelcast.simulator.protocol.CoordinatorClient;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.utils.BashCommand;
import com.hazelcast.simulator.utils.CommandLineExitException;
import com.hazelcast.simulator.utils.CommonUtils;
import com.hazelcast.simulator.worker.operations.ExecuteScriptOperation;
import org.apache.log4j.Logger;

import java.io.Closeable;
import java.io.File;
import java.rmi.AlreadyBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import static com.hazelcast.simulator.coordinator.AgentUtils.startAgents;
import static com.hazelcast.simulator.coordinator.AgentUtils.stopAgents;
import static com.hazelcast.simulator.coordinator.registry.AgentData.publicAddresses;
import static com.hazelcast.simulator.drivers.Driver.loadDriver;
import static com.hazelcast.simulator.utils.CommonUtils.sleepSeconds;
import static com.hazelcast.simulator.utils.FileUtils.getConfigurationFile;
import static com.hazelcast.simulator.utils.FileUtils.getUserDir;
import static com.hazelcast.simulator.utils.FormatUtils.join;
import static com.hazelcast.simulator.utils.TagUtils.matches;
import static java.lang.Runtime.getRuntime;
import static java.lang.String.format;

@SuppressWarnings({"checkstyle:classdataabstractioncoupling", "checkstyle:classfanoutcomplexity"})
public class Coordinator implements Closeable {

    private static final Logger LOGGER = Logger.getLogger(Coordinator.class);

    private final PerformanceStatsCollector performanceStatsCollector = new PerformanceStatsCollector();

    private final Registry registry;
    private final CoordinatorParameters parameters;
    private final FailureCollector failureCollector;
    private final SimulatorProperties properties;
    private final int testCompletionTimeoutSeconds;
    private final CoordinatorClient client;
    private CoordinatorRemoteImpl coordinatorRemote;

    public Coordinator(Registry registry, CoordinatorParameters parameters) {
        this.registry = registry;
        this.parameters = parameters;
        this.failureCollector = new FailureCollector(parameters.getOutputDirectory(), registry);
        this.properties = parameters.getSimulatorProperties();
        this.testCompletionTimeoutSeconds = properties.getTestCompletionTimeoutSeconds();

        this.client = new CoordinatorClient()
                .setAgentBrokerPort(properties.getAgentPort())
                .setProcessor(new CoordinatorOperationProcessor(failureCollector, performanceStatsCollector))
                .setFailureCollector(failureCollector);
    }

    FailureCollector getFailureCollector() {
        return failureCollector;
    }

    public void start() throws Exception {
        client.start();

        registerShutdownHook();

        logConfiguration();

        log("Coordinator starting...");

        startAgents(properties, registry);

        startClient();

        new PrepareSessionTask(
                publicAddresses(registry.getAgents()),
                properties.asMap(),
                new File(getUserDir(), "upload").getAbsoluteFile(),
                parameters.getSessionId()).run();

        installDriver(properties.getVersionSpec());

        initCoordinatorRemote();

        log("Coordinator started...");
    }

    private void initCoordinatorRemote() throws RemoteException, AlreadyBoundException {
        int remotePort = properties.getCoordinatorPort();
        if (remotePort != 0) {
            java.rmi.registry.Registry rmiRegistry = LocateRegistry.createRegistry(remotePort);
            coordinatorRemote = new CoordinatorRemoteImpl(this);
            Remote stub = UnicastRemoteObject.exportObject(coordinatorRemote, 0);

            // Bind the remote object's stub in the registry
            rmiRegistry.bind("CoordinatorRemote", stub);
        }
    }

    private void registerShutdownHook() {
        if (parameters.skipShutdownHook()) {
            return;
        }

        getRuntime().addShutdownHook(new Thread(() -> {
            try {
                close();
            } catch (Throwable t) {
                LOGGER.fatal(t.getMessage(), t);
            }
        }));
    }

    private void logConfiguration() {
        log("Total number of agents: %s", registry.agentCount());
        log("Output directory: " + parameters.getOutputDirectory().getAbsolutePath());

        int performanceIntervalSeconds
                = parameters.getSimulatorProperties().getInt("WORKER_PERFORMANCE_MONITOR_INTERVAL_SECONDS");

        if (performanceIntervalSeconds > 0) {
            log("Performance monitor enabled (%d seconds interval)", performanceIntervalSeconds);
        } else {
            log("Performance monitor disabled");
        }

        if (properties.getCoordinatorPort() > 0) {
            log("Coordinator remote enabled on port " + properties.getCoordinatorPort());
        }
    }

    @Override
    public void close() {
        stopTests();

        new TerminateWorkersTask(properties, registry, client).run();

        client.close();

        stopAgents(properties, registry);

        if (!parameters.skipDownload()) {
            new DownloadTask(
                    publicAddresses(registry.getAgents()),
                    properties.asMap(),
                    parameters.getOutputDirectory().getParentFile(),
                    parameters.getSessionId()).run();
        }

        failureCollector.logFailureInfo();
    }

    private void stopTests() {
        Collection<TestData> tests = registry.getTests();
        for (TestData test : tests) {
            test.setStopRequested(true);
        }

        for (int i = 0; i < testCompletionTimeoutSeconds; i++) {
            tests.removeIf(TestData::isCompleted);

            sleepSeconds(1);

            if (tests.isEmpty()) {
                return;
            }
        }

        LOGGER.info("The following tests failed to complete: " + tests);
    }

    private void startClient() {
        // todo: should be async to speed things up
        for (AgentData agent : registry.getAgents()) {
            try {
                client.connectToAgentBroker(agent.getAddress(), agent.getPublicAddress());
            } catch (Exception e) {
                LOGGER.debug(e.getMessage(), e);
                throw new CommandLineExitException("Failed to connect to agent [" + agent.getPublicAddress() + "], "
                        + "cause [" + e.getMessage() + "]");
            }
        }

        LOGGER.info("Remote client started successfully!");
    }

    public void download() {
        new DownloadTask(publicAddresses(registry.getAgents()),
                properties.asMap(),
                parameters.getOutputDirectory().getParentFile(),
                parameters.getSessionId()).run();

    }

    public void stop() {
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

    public void installDriver(String versionSpec) {
        new BashCommand(getConfigurationFile("upload-driver.sh").getAbsolutePath())
                .addParams(join((publicAddresses(registry.getAgents())), ","))
                .ensureJavaOnPath()
                .addEnvironment(properties.asMap())
                .execute();

        loadDriver(properties.get("DRIVER"))
                .setAll(properties.asMap())
                .set("VERSION_SPEC", versionSpec)
                .setAgents(registry.getAgents())
                .install();
    }

    public String printLayout() {
        return registry.printLayout();
    }

    public String testRun(RcTestRunOperation op) {
        LOGGER.info("Run starting...");
        final RunTestSuiteTask runTestSuiteTask = createRunTestSuiteTask(op.getTestSuite());

        if (op.isAsync()) {
            if (op.getTestSuite().size() > 1) {
                throw new IllegalArgumentException("1 test in testsuite allowed");
            }

            new Thread(runTestSuiteTask::run).start();

            for (; ; ) {
                sleepSeconds(1);
                for (TestData test : registry.getTests()) {
                    if (test.getTestSuite() == op.getTestSuite()) {
                        return test.getTestCase().getId();
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
        LOGGER.info(format("Test [%s] stopping...", op.getTestId()));

        TestData test = registry.getTest(op.getTestId());
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

    public String testStatus(RcTestStatusOperation op) {
        TestData test = registry.getTest(op.getTestId());
        return test == null ? "null" : test.getStatusString();
    }

    public String workerStart(RcWorkerStartOperation op) throws Exception {
        // todo: tags
        String workerType = op.getWorkerType();

        LOGGER.info("Starting " + op.getCount() + " [" + workerType + "] workers...");

        Driver driver = loadDriver(properties.get("DRIVER"))
                .setAgents(registry.getAgents())
                .setAll(properties.asMap())
                .set("CLIENT_ARGS", op.getVmOptions())
                .set("MEMBER_ARGS", op.getVmOptions())
                .set("SESSION_ID", parameters.getSessionId())
                .setIfNotNull("VERSION_SPEC", op.getVersionSpec())
                .setIfNotNull("CONFIG", op.getConfig());

        List<AgentData> agents = findAgents(op);
        if (agents.isEmpty()) {
            throw new IllegalStateException("No suitable agents found");
        }

        LOGGER.info("Suitable agents: " + agents);

        DeploymentPlan deploymentPlan = new DeploymentPlan(driver, agents)
                .addToPlan(op.getCount(), workerType);

        List<WorkerData> workers = createStartWorkersTask(deploymentPlan.getWorkerDeployment(), op.getTags()).run();
        LOGGER.info("Workers started!");
        return WorkerData.toAddressString(workers);
    }

    private List<AgentData> findAgents(RcWorkerStartOperation op) {
        List<AgentData> agents = new ArrayList<>(registry.getAgents());
        List<AgentData> result = new ArrayList<>();
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

            result.add(agent);
        }
        return result;
    }

    public String workerKill(RcWorkerKillOperation op) throws Exception {
        WorkerQuery workerQuery = op.getWorkerQuery();

        LOGGER.info(format("Killing %s...", workerQuery));

        List<WorkerData> result = new KillWorkersTask(
                registry,
                client,
                op.getCommand(),
                workerQuery,
                properties.getInt("WAIT_FOR_WORKER_SHUTDOWN_TIMEOUT_SECONDS")
        ).run();

        LOGGER.info("\n" + registry.printLayout());

        LOGGER.info(format("Killing %s complete", workerQuery));

        return WorkerData.toAddressString(result);
    }

    public String workerScript(RcWorkerScriptOperation operation) throws Exception {
        List<WorkerData> workers = operation.getWorkerQuery().execute(registry.getWorkers());

        LOGGER.info(format("Script [%s] on %s workers ...", operation.getCommand(), workers.size()));

        Map<WorkerData, Future<String>> futures = new HashMap<>();
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

    StartWorkersTask createStartWorkersTask(Map<SimulatorAddress, List<WorkerParameters>> deploymentPlan,
                                            Map<String, String> workerTags) {
        return new StartWorkersTask(
                deploymentPlan,
                workerTags,
                client,
                registry,
                parameters.getWorkerVmStartupDelayMs());
    }

    RunTestSuiteTask createRunTestSuiteTask(TestSuite testSuite) {
        return new RunTestSuiteTask(testSuite,
                parameters,
                registry,
                failureCollector,
                client,
                performanceStatsCollector);
    }


    private static void log(String message, Object... args) {
        String log = message == null ? "null" : format(message, args);
        LOGGER.info(log);
    }
}
