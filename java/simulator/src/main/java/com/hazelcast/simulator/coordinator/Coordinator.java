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
import com.hazelcast.simulator.coordinator.registry.AgentData;
import com.hazelcast.simulator.coordinator.registry.Registry;
import com.hazelcast.simulator.coordinator.registry.TestData;
import com.hazelcast.simulator.coordinator.tasks.PrepareRunTask;
import com.hazelcast.simulator.coordinator.tasks.RunTestSuiteTask;
import com.hazelcast.simulator.coordinator.tasks.StartWorkersTask;
import com.hazelcast.simulator.coordinator.tasks.TerminateWorkersTask;
import com.hazelcast.simulator.protocol.CoordinatorClient;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.utils.CommandLineExitException;
import com.hazelcast.simulator.utils.CommonUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Closeable;
import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static com.hazelcast.simulator.coordinator.AgentUtils.startAgents;
import static com.hazelcast.simulator.coordinator.AgentUtils.stopAgents;
import static com.hazelcast.simulator.drivers.Driver.loadDriver;
import static com.hazelcast.simulator.utils.CommonUtils.sleepSeconds;
import static com.hazelcast.simulator.utils.FileUtils.getUserDir;
import static com.hazelcast.simulator.utils.FileUtils.locatePythonFile;
import static java.lang.Runtime.getRuntime;
import static java.lang.String.format;

@SuppressWarnings({"checkstyle:classdataabstractioncoupling", "checkstyle:classfanoutcomplexity"})
public class Coordinator implements Closeable {

    private static final Logger LOGGER = LogManager.getLogger(Coordinator.class);
    private static final int PARALLEL_CONNECT = 16;

    private final PerformanceStatsCollector performanceStatsCollector = new PerformanceStatsCollector();
    private final Registry registry;
    private final CoordinatorParameters parameters;
    private final FailureCollector failureCollector;
    private final SimulatorProperties properties;
    private final int testCompletionTimeoutSeconds;
    private final CoordinatorClient client;

    public Coordinator(Registry registry, CoordinatorParameters parameters) {
        this.registry = registry;
        this.parameters = parameters;
        this.failureCollector = new FailureCollector(parameters.getSimulatorProperties().get("run_path"), registry);
        this.properties = parameters.getSimulatorProperties();
        this.testCompletionTimeoutSeconds = properties.getTestCompletionTimeoutSeconds();

        this.client = new CoordinatorClient()
                .setAgentBrokerPort(properties.getAgentPort())
                .setProcessor(new CoordinatorMessageHandler(failureCollector, performanceStatsCollector))
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

        startAgents(registry);

        startClient();

        new PrepareRunTask(
                registry.getAgents(),
                properties.asMap(),
                new File(getUserDir(), "upload").getAbsoluteFile()).run();

        log("Coordinator started...");
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
        String runPath = parameters.getSimulatorProperties().get("run_path");
        log("Run path: " + new File(runPath).getAbsolutePath());

        int performanceIntervalSeconds
                = parameters.getSimulatorProperties().getInt("performance_monitor_interval_seconds");

        if (performanceIntervalSeconds > 0) {
            log("Performance monitor enabled (%d seconds interval)", performanceIntervalSeconds);
        } else {
            log("Performance monitor disabled");
        }
    }

    @Override
    public void close() {
        stopTests();

        new TerminateWorkersTask(properties, registry, client).run();

        client.close();

        stopAgents(registry);

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

    private void startClient() throws ExecutionException, InterruptedException {
       ExecutorService executor = Executors.newFixedThreadPool(PARALLEL_CONNECT);
        List<Future> futures = new ArrayList<>();
        for (AgentData agent : registry.getAgents()) {
            Future f = executor.submit(() -> {
                try {
                    client.connectToAgentBroker(agent.getAddress(), agent.getPublicAddress());
                } catch (Exception e) {
                    LOGGER.debug(e.getMessage(), e);
                    throw new CommandLineExitException("Failed to connect to agent [" + agent.getPublicAddress() + "], "
                            + "cause [" + e.getMessage() + "]");
                }
            });
            futures.add(f);
        }

        for (Future future : futures) {
            try {
                future.get();
            } catch (ExecutionException e) {
                if (e.getCause() instanceof CommandLineExitException) {
                    throw new CommandLineExitException(e.getCause());
                }else{
                    throw e;
                }
            }
        }

        LOGGER.info("Remote client started successfully!");
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
