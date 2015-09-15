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

import com.hazelcast.simulator.common.AgentsFile;
import com.hazelcast.simulator.common.GitInfo;
import com.hazelcast.simulator.common.JavaProfiler;
import com.hazelcast.simulator.common.SimulatorProperties;
import com.hazelcast.simulator.coordinator.remoting.AgentsClient;
import com.hazelcast.simulator.protocol.connector.CoordinatorConnector;
import com.hazelcast.simulator.protocol.registry.AgentData;
import com.hazelcast.simulator.protocol.registry.ComponentRegistry;
import com.hazelcast.simulator.provisioner.Bash;
import com.hazelcast.simulator.test.TestCase;
import com.hazelcast.simulator.test.TestPhase;
import com.hazelcast.simulator.test.TestSuite;
import com.hazelcast.simulator.utils.CommandLineExitException;
import com.hazelcast.simulator.utils.ThreadSpawner;
import org.apache.log4j.Logger;

import java.io.File;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.simulator.coordinator.CoordinatorUtils.createAddressConfig;
import static com.hazelcast.simulator.coordinator.CoordinatorUtils.getMaxTestCaseIdLength;
import static com.hazelcast.simulator.coordinator.CoordinatorUtils.initMemberLayout;
import static com.hazelcast.simulator.protocol.configuration.Ports.AGENT_PORT;
import static com.hazelcast.simulator.utils.CloudProviderUtils.isEC2;
import static com.hazelcast.simulator.utils.CommonUtils.exitWithError;
import static com.hazelcast.simulator.utils.CommonUtils.getSimulatorVersion;
import static com.hazelcast.simulator.utils.CommonUtils.secondsToHuman;
import static com.hazelcast.simulator.utils.CommonUtils.sleepSeconds;
import static com.hazelcast.simulator.utils.ExecutorFactory.createFixedThreadPool;
import static com.hazelcast.simulator.utils.FileUtils.ensureExistingFile;
import static com.hazelcast.simulator.utils.FileUtils.getFilesFromClassPath;
import static com.hazelcast.simulator.utils.FileUtils.getSimulatorHome;
import static com.hazelcast.simulator.utils.HarakiriMonitorUtils.getStartHarakiriMonitorCommandOrNull;
import static java.lang.String.format;

public final class Coordinator {

    static final File SIMULATOR_HOME = getSimulatorHome();
    static final File WORKING_DIRECTORY = new File(System.getProperty("user.dir"));
    static final File UPLOAD_DIRECTORY = new File(WORKING_DIRECTORY, "upload");

    private static final int COOLDOWN_SECONDS = 10;
    private static final int TEST_CASE_RUNNER_SLEEP_PERIOD = 30;
    private static final int EXECUTOR_TERMINATION_TIMEOUT_SECONDS = 10;

    private static final Logger LOGGER = Logger.getLogger(Coordinator.class);

    int cooldownSeconds = COOLDOWN_SECONDS;
    int testCaseRunnerSleepPeriod = TEST_CASE_RUNNER_SLEEP_PERIOD;

    private final ComponentRegistry componentRegistry = new ComponentRegistry();

    private final CoordinatorParameters parameters;
    private final TestSuite testSuite;

    private final SimulatorProperties props;
    private final Bash bash;

    private AgentsClient agentsClient;
    private CoordinatorConnector coordinatorConnector;

    private FailureMonitor failureMonitor;
    private PerformanceMonitor performanceMonitor;

    private ExecutorService parallelExecutor;

    public Coordinator(CoordinatorParameters parameters, TestSuite testSuite) {
        this.parameters = parameters;
        this.testSuite = testSuite;

        this.props = parameters.getSimulatorProperties();
        this.bash = new Bash(props);
    }

    CoordinatorParameters getParameters() {
        return parameters;
    }

    TestSuite getTestSuite() {
        return testSuite;
    }

    private void run() throws Exception {
        try {
            initAgents();

            startWorkers();

            failureMonitor = new FailureMonitor(agentsClient, testSuite.id);
            failureMonitor.start();

            performanceMonitor = new PerformanceMonitor(agentsClient);

            runTestSuite();

            logFailureInfo();
        } finally {
            if (performanceMonitor != null) {
                performanceMonitor.stop();
            }

            if (failureMonitor != null) {
                failureMonitor.stop();
            }

            if (parallelExecutor != null) {
                parallelExecutor.shutdown();
                parallelExecutor.awaitTermination(EXECUTOR_TERMINATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            }

            if (agentsClient != null) {
                agentsClient.terminateWorkers();
                agentsClient.stop();
            }

            if (coordinatorConnector != null) {
                coordinatorConnector.shutdown();
            }

            killAgents();
        }
    }

    private void initAgents() {
        populateComponentRegister();

        startAgents();

        agentsClient = new AgentsClient(componentRegistry.getAgents());
        agentsClient.start();

        try {
            startCoordinatorConnector();
        } catch (Exception e) {
            throw new CommandLineExitException("Could not start CoordinatorConnector", e);
        }

        initMemberWorkerCount();
        initMemberHzConfig();
        initClientHzConfig();

        int agentCount = agentsClient.getAgentCount();
        LOGGER.info(format("Performance monitor enabled: %s", parameters.isMonitorPerformance()));
        LOGGER.info(format("Total number of agents: %s", agentCount));
        LOGGER.info(format("Total number of Hazelcast member workers: %s", parameters.getMemberWorkerCount()));
        LOGGER.info(format("Total number of Hazelcast client workers: %s", parameters.getClientWorkerCount()));

        try {
            agentsClient.initTestSuite(testSuite);
        } catch (Exception e) {
            throw new CommandLineExitException("Could not init TestSuite", e);
        }

        uploadUploadDirectory();
        uploadWorkerClassPath();
        uploadYourKitIfNeeded();
        // TODO: copy the Hazelcast JARs
    }

    private void populateComponentRegister() {
        File agentsFile = parameters.getAgentsFile();
        ensureExistingFile(agentsFile);
        AgentsFile.load(agentsFile, componentRegistry);
        if (componentRegistry.agentCount() == 0) {
            throw new CommandLineExitException("Agents file " + agentsFile + " is empty.");
        }
    }

    private void startAgents() {
        echoLocal("Starting %s Agents", componentRegistry.agentCount());

        ThreadSpawner spawner = new ThreadSpawner("startAgents", true);
        for (final AgentData agentData : componentRegistry.getAgents()) {
            spawner.spawn(new Runnable() {
                @Override
                public void run() {
                    startAgent(agentData.getAddressIndex(), agentData.getPublicAddress());
                }
            });
        }
        spawner.awaitCompletion();

        echoLocal("Successfully started agents on %s boxes", componentRegistry.agentCount());
    }

    private void startAgent(int addressIndex, String ip) {
        echoLocal("Killing Java processes on %s", ip);
        bash.killAllJavaProcesses(ip);

        echoLocal("Starting Agent on %s", ip);
        String mandatoryParameters = format("--addressIndex %d --publicAddress %s", addressIndex, ip);
        String optionalParameters = "";
        if (isEC2(props.get("CLOUD_PROVIDER"))) {
            optionalParameters = format(" --cloudProvider %s --cloudIdentity %s --cloudCredential %s",
                    props.get("CLOUD_PROVIDER"),
                    props.get("CLOUD_IDENTITY"),
                    props.get("CLOUD_CREDENTIAL"));
        }
        bash.ssh(ip, format("nohup hazelcast-simulator-%s/bin/agent %s%s > agent.out 2> agent.err < /dev/null &",
                getSimulatorVersion(), mandatoryParameters, optionalParameters));
    }

    private void startCoordinatorConnector() {
        coordinatorConnector = new CoordinatorConnector();
        ThreadSpawner spawner = new ThreadSpawner("startCoordinatorConnector", true);
        for (final AgentData agentData : componentRegistry.getAgents()) {
            spawner.spawn(new Runnable() {
                @Override
                public void run() {
                    coordinatorConnector.addAgent(agentData.getAddressIndex(), agentData.getPublicAddress(), AGENT_PORT);
                }
            });
        }
        spawner.awaitCompletion();
    }

    private void initMemberWorkerCount() {
        if (parameters.getMemberWorkerCount() == -1) {
            parameters.setMemberWorkerCount(agentsClient.getAgentCount());
        }
    }

    private void initMemberHzConfig() {
        String addressConfig = createAddressConfig("member", agentsClient.getPrivateAddresses(), parameters);

        String memberHzConfig = parameters.getMemberHzConfig();
        memberHzConfig = memberHzConfig.replace("<!--MEMBERS-->", addressConfig);

        String manCenterURL = props.get("MANAGEMENT_CENTER_URL").trim();
        if (!"none".equals(manCenterURL) && (manCenterURL.startsWith("http://") || manCenterURL.startsWith("https://"))) {
            String updateInterval = props.get("MANAGEMENT_CENTER_UPDATE_INTERVAL").trim();
            String updateIntervalAttr = (updateInterval.isEmpty()) ? "" : " update-interval=\"" + updateInterval + "\"";
            memberHzConfig = memberHzConfig.replace("<!--MANAGEMENT_CENTER_CONFIG-->",
                    format("<management-center enabled=\"true\"%s>%n        %s%n" + "    </management-center>%n",
                            updateIntervalAttr, manCenterURL));
        }
        parameters.setMemberHzConfig(memberHzConfig);
    }

    private void initClientHzConfig() {
        String addressConfig = createAddressConfig("address", agentsClient.getPrivateAddresses(), parameters);

        String clientHzConfig = parameters.getClientHzConfig().replace("<!--MEMBERS-->", addressConfig);
        parameters.setClientHzConfig(clientHzConfig);
    }

    private void uploadUploadDirectory() {
        try {
            if (!UPLOAD_DIRECTORY.exists()) {
                LOGGER.debug("Skipping upload, since no upload file in working directory");
                return;
            }

            LOGGER.info(format("Starting uploading '%s' to agents", UPLOAD_DIRECTORY.getAbsolutePath()));
            List<File> files = getFilesFromClassPath(UPLOAD_DIRECTORY.getAbsolutePath());
            for (String ip : agentsClient.getPublicAddresses()) {
                LOGGER.info(format("Uploading '%s' to agent %s", UPLOAD_DIRECTORY.getAbsolutePath(), ip));
                for (File file : files) {
                    bash.execute(format("rsync -avv -e \"ssh %s\" %s %s@%s:hazelcast-simulator-%s/workers/%s/",
                            props.get("SSH_OPTIONS", ""),
                            file,
                            props.get("USER"),
                            ip,
                            getSimulatorVersion(),
                            testSuite.id));
                }
                LOGGER.info("    " + ip + " copied");
            }
            LOGGER.info(format("Finished uploading '%s' to agents", UPLOAD_DIRECTORY.getAbsolutePath()));
        } catch (Exception e) {
            throw new CommandLineExitException("Could not copy upload directory to agents", e);
        }
    }

    private void uploadWorkerClassPath() {
        String workerClassPath = parameters.getWorkerClassPath();
        if (workerClassPath == null) {
            return;
        }

        try {
            List<File> upload = getFilesFromClassPath(workerClassPath);
            LOGGER.info(format("Copying %d files from workerClasspath '%s' to agents", upload.size(), workerClassPath));
            for (String ip : agentsClient.getPublicAddresses()) {
                for (File file : upload) {
                    bash.execute(
                            format("rsync --ignore-existing -avv -e \"ssh %s\" %s %s@%s:hazelcast-simulator-%s/workers/%s/lib",
                                    props.get("SSH_OPTIONS", ""),
                                    file.getAbsolutePath(),
                                    props.get("USER"),
                                    ip,
                                    getSimulatorVersion(),
                                    testSuite.id));
                }
                LOGGER.info("    " + ip + " copied");
            }
            LOGGER.info(format("Finished copying workerClasspath '%s' to agents", workerClassPath));
        } catch (Exception e) {
            throw new CommandLineExitException("Could not upload worker classpath to agents", e);
        }
    }

    private void uploadYourKitIfNeeded() {
        if (parameters.getProfiler() != JavaProfiler.YOURKIT) {
            return;
        }

        // TODO: in the future we'll only upload the requested YourKit library (32 or 64 bit)
        LOGGER.info("Uploading YourKit dependencies to agents");
        for (String ip : agentsClient.getPublicAddresses()) {
            bash.ssh(ip, format("mkdir -p hazelcast-simulator-%s/yourkit", getSimulatorVersion()));

            bash.execute(format("rsync --ignore-existing -avv -e \"ssh %s\" %s/yourkit %s@%s:hazelcast-simulator-%s/",
                    props.get("SSH_OPTIONS", ""),
                    getSimulatorHome().getAbsolutePath(),
                    props.get("USER"),
                    ip,
                    getSimulatorVersion()));
        }
    }

    private void startWorkers() {
        List<AgentMemberLayout> agentMemberLayouts = initMemberLayout(componentRegistry, parameters);

        long started = System.nanoTime();
        int totalWorkerCount = parameters.getMemberWorkerCount() + parameters.getClientWorkerCount();
        try {
            echo("Killing all remaining workers");
            agentsClient.terminateWorkers();
            echo("Successfully killed all remaining workers");

            echo("Starting %d workers", totalWorkerCount);
            agentsClient.spawnWorkers(agentMemberLayouts);
            echo("Successfully started workers");
        } catch (Exception e) {
            throw new CommandLineExitException("Failed to start workers", e);
        }

        long durationMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
        LOGGER.info((format("Successfully started a grand total of %s Workers JVMs after %s ms", totalWorkerCount, durationMs)));
    }

    void runTestSuite() {
        echo("Starting testsuite: %s", testSuite.id);
        echo("Tests in testsuite: %s", testSuite.size());
        echo("Running time per test: %s ", secondsToHuman(testSuite.durationSeconds));
        echo("Expected total testsuite time: %s", secondsToHuman(testSuite.size() * testSuite.durationSeconds));

        long started = System.nanoTime();

        if (parameters.isParallel()) {
            runParallel();
        } else {
            runSequential();
        }

        terminateWorkers();

        // the coordinator needs to sleep some time to make sure that it will get failures if they are there
        LOGGER.info("Starting cool down (10 sec)");
        sleepSeconds(cooldownSeconds);
        LOGGER.info("Finished cool down");

        long duration = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - started);
        LOGGER.info(format("Total running time: %s seconds", duration));
    }

    private void runParallel() {
        echo("Running %s tests parallel", testSuite.size());

        final int maxTestCaseIdLength = getMaxTestCaseIdLength(testSuite.testCaseList);
        final ConcurrentMap<TestPhase, CountDownLatch> testPhaseSyncMap = getTestPhaseSyncMap(testSuite.testCaseList.size());

        parallelExecutor = createFixedThreadPool(testSuite.size(), Coordinator.class);

        List<Future> futures = new LinkedList<Future>();
        for (final TestCase testCase : testSuite.testCaseList) {
            Future future = parallelExecutor.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        TestCaseRunner runner = new TestCaseRunner(testCase, testSuite, Coordinator.this, agentsClient,
                                failureMonitor, performanceMonitor, maxTestCaseIdLength, testPhaseSyncMap);
                        boolean success = runner.run();
                        if (!success && testSuite.failFast) {
                            LOGGER.info("Aborting testsuite due to failure (not implemented yet)");
                            // FIXME: we should abort here as logged
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            });
            futures.add(future);
        }
        try {
            for (Future future : futures) {
                future.get();
            }
        } catch (Exception e) {
            throw new CommandLineExitException("Could not execute tests in parallel", e);
        }
    }

    private void runSequential() {
        echo("Running %s tests sequentially", testSuite.size());

        int maxTestCaseIdLength = getMaxTestCaseIdLength(testSuite.testCaseList);

        for (TestCase testCase : testSuite.testCaseList) {
            TestCaseRunner runner = new TestCaseRunner(testCase, testSuite, this, agentsClient,
                    failureMonitor, performanceMonitor, maxTestCaseIdLength, null);
            boolean success = runner.run();
            if (!success && testSuite.failFast) {
                LOGGER.info("Aborting testsuite due to failure");
                break;
            }
            if (!success || parameters.isRefreshJvm()) {
                terminateWorkers();
                startWorkers();
            }
        }
    }

    private ConcurrentMap<TestPhase, CountDownLatch> getTestPhaseSyncMap(int testCount) {
        ConcurrentMap<TestPhase, CountDownLatch> testPhaseSyncMap = new ConcurrentHashMap<TestPhase, CountDownLatch>();
        boolean useTestCount = true;
        for (TestPhase testPhase : TestPhase.values()) {
            testPhaseSyncMap.put(testPhase, new CountDownLatch(useTestCount ? testCount : 0));
            if (testPhase == parameters.getLastTestPhaseToSync()) {
                useTestCount = false;
            }
        }
        return testPhaseSyncMap;
    }

    private void terminateWorkers() {
        try {
            echo("Terminating workers");
            agentsClient.terminateWorkers();
            echo("All workers have been terminated");
        } catch (Exception e) {
            LOGGER.fatal("Could not terminate workers!", e);
        }
    }

    private void logFailureInfo() {
        int failureCount = failureMonitor.getFailureCount();
        if (failureCount > 0) {
            LOGGER.fatal("-----------------------------------------------------------------------------");
            LOGGER.fatal(failureCount + " failures have been detected!!!");
            LOGGER.fatal("-----------------------------------------------------------------------------");
            throw new CommandLineExitException(failureCount + " failures have been detected");
        }
        LOGGER.info("-----------------------------------------------------------------------------");
        LOGGER.info("No failures have been detected!");
        LOGGER.info("-----------------------------------------------------------------------------");
    }

    private void killAgents() {
        ThreadSpawner spawner = new ThreadSpawner("killAgents", true);
        final String startHarakiriMonitorCommand = getStartHarakiriMonitorCommandOrNull(props);

        echoLocal("Killing %s Agents", componentRegistry.agentCount());
        for (final AgentData agentData : componentRegistry.getAgents()) {
            spawner.spawn(new Runnable() {
                @Override
                public void run() {
                    echoLocal("Killing Agent %s", agentData.getPublicAddress());
                    bash.killAllJavaProcesses(agentData.getPublicAddress());

                    if (startHarakiriMonitorCommand != null) {
                        LOGGER.info(format("Starting HarakiriMonitor on %s", agentData.getPublicAddress()));
                        bash.ssh(agentData.getPublicAddress(), startHarakiriMonitorCommand);
                    }
                }
            });
        }
        spawner.awaitCompletion();
        echoLocal("Successfully killed %s Agents", componentRegistry.agentCount());
    }

    private void echoLocal(String msg, Object... args) {
        LOGGER.info(format(msg, args));
    }

    private void echo(String msg, Object... args) {
        echo(format(msg, args));
    }

    private void echo(String msg) {
        agentsClient.echo(msg);
        LOGGER.info(msg);
    }

    public static void main(String[] args) {
        try {
            LOGGER.info("Hazelcast Simulator Coordinator");
            LOGGER.info(format("Version: %s, Commit: %s, Build Time: %s",
                    getSimulatorVersion(), GitInfo.getCommitIdAbbrev(), GitInfo.getBuildTime()));
            LOGGER.info(format("SIMULATOR_HOME: %s", SIMULATOR_HOME));

            Coordinator coordinator = CoordinatorCli.init(args);

            LOGGER.info(format("Loading agents file: %s", coordinator.parameters.getAgentsFile().getAbsolutePath()));
            LOGGER.info(format("HAZELCAST_VERSION_SPEC: %s", coordinator.props.getHazelcastVersionSpec()));

            coordinator.run();
        } catch (Exception e) {
            exitWithError(LOGGER, "Failed to run testsuite", e);
        }
    }
}
