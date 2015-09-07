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

import com.hazelcast.simulator.agent.workerjvm.WorkerJvmSettings;
import com.hazelcast.simulator.common.AgentAddress;
import com.hazelcast.simulator.common.AgentsFile;
import com.hazelcast.simulator.common.GitInfo;
import com.hazelcast.simulator.common.SimulatorProperties;
import com.hazelcast.simulator.coordinator.remoting.AgentsClient;
import com.hazelcast.simulator.provisioner.Bash;
import com.hazelcast.simulator.test.TestCase;
import com.hazelcast.simulator.test.TestPhase;
import com.hazelcast.simulator.test.TestSuite;
import com.hazelcast.simulator.utils.CommandLineExitException;
import org.apache.log4j.Logger;

import java.io.File;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.simulator.coordinator.CoordinatorHelper.assignDedicatedMemberMachines;
import static com.hazelcast.simulator.coordinator.CoordinatorHelper.createAddressConfig;
import static com.hazelcast.simulator.coordinator.CoordinatorHelper.findNextAgentLayout;
import static com.hazelcast.simulator.coordinator.CoordinatorHelper.getMaxTestCaseIdLength;
import static com.hazelcast.simulator.coordinator.CoordinatorHelper.getStartHarakiriMonitorCommand;
import static com.hazelcast.simulator.coordinator.CoordinatorHelper.initAgentMemberLayouts;
import static com.hazelcast.simulator.utils.CloudProviderUtils.isEC2;
import static com.hazelcast.simulator.utils.CommonUtils.exitWithError;
import static com.hazelcast.simulator.utils.CommonUtils.getSimulatorVersion;
import static com.hazelcast.simulator.utils.CommonUtils.secondsToHuman;
import static com.hazelcast.simulator.utils.CommonUtils.sleepSeconds;
import static com.hazelcast.simulator.utils.ExecutorFactory.createFixedThreadPool;
import static com.hazelcast.simulator.utils.FileUtils.ensureExistingFile;
import static com.hazelcast.simulator.utils.FileUtils.getFilesFromClassPath;
import static com.hazelcast.simulator.utils.FileUtils.getSimulatorHome;
import static java.lang.String.format;

public final class Coordinator {

    static final File SIMULATOR_HOME = getSimulatorHome();
    static final File WORKING_DIRECTORY = new File(System.getProperty("user.dir"));
    static final File UPLOAD_DIRECTORY = new File(WORKING_DIRECTORY, "upload");

    private static final int COOLDOWN_SECONDS = 10;
    private static final int TEST_CASE_RUNNER_SLEEP_PERIOD = 30;
    private static final int EXECUTOR_TERMINATION_TIMEOUT_SECONDS = 10;

    private static final Logger LOGGER = Logger.getLogger(Coordinator.class);

    // options
    boolean monitorPerformance;
    boolean verifyEnabled = true;
    String workerClassPath;
    Integer testStopTimeoutMs;
    File agentsFile;
    TestSuite testSuite;
    int dedicatedMemberMachineCount;
    boolean parallel;
    TestPhase lastTestPhaseToSync;
    WorkerJvmSettings workerJvmSettings;
    int cooldownSeconds = COOLDOWN_SECONDS;
    int testCaseRunnerSleepPeriod = TEST_CASE_RUNNER_SLEEP_PERIOD;

    // internal state
    final SimulatorProperties props = new SimulatorProperties();

    AgentsClient agentsClient;
    FailureMonitor failureMonitor;
    PerformanceMonitor performanceMonitor;

    private final List<AgentAddress> addresses = Collections.synchronizedList(new LinkedList<AgentAddress>());

    private Bash bash;
    private ExecutorService parallelExecutor;

    private void run() throws Exception {
        try {
            bash = new Bash(props);

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

            killAgents();
        }
    }

    private void initAgents() {
        ensureExistingFile(agentsFile);
        addresses.addAll(AgentsFile.load(agentsFile));
        if (addresses.isEmpty()) {
            throw new CommandLineExitException("Agents file " + agentsFile + " is empty.");
        }

        startAgents();

        agentsClient = new AgentsClient(addresses);
        agentsClient.start();

        initMemberWorkerCount(workerJvmSettings);
        initHzConfig(workerJvmSettings);
        initClientHzConfig(workerJvmSettings);

        int agentCount = agentsClient.getAgentCount();
        LOGGER.info(format("Performance monitor enabled: %s", monitorPerformance));
        LOGGER.info(format("Total number of agents: %s", agentCount));
        LOGGER.info(format("Total number of Hazelcast member workers: %s", workerJvmSettings.memberWorkerCount));
        LOGGER.info(format("Total number of Hazelcast client workers: %s", workerJvmSettings.clientWorkerCount));

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

    private void startAgents() {
        echoLocal("Starting %s Agents", addresses.size());

        for (AgentAddress address : addresses) {
            startAgent(address.publicAddress);
        }

        echoLocal("Successfully started agents on %s boxes", addresses.size());
    }

    private void startAgent(String ip) {
        echoLocal("Killing Java processes on %s", ip);
        bash.killAllJavaProcesses(ip);

        echoLocal("Starting Agent on %s", ip);
        String additionalParameters = "";
        if (isEC2(props.get("CLOUD_PROVIDER"))) {
            additionalParameters = format("--cloudProvider %s --cloudIdentity %s --cloudCredential %s",
                    props.get("CLOUD_PROVIDER"),
                    props.get("CLOUD_IDENTITY"),
                    props.get("CLOUD_CREDENTIAL"));
        }
        bash.ssh(ip, format(
                "nohup hazelcast-simulator-%s/bin/agent %s > agent.out 2> agent.err < /dev/null &",
                getSimulatorVersion(),
                additionalParameters));
    }

    private void killAgents() {
        String startHarakiriMonitorCommand = getStartHarakiriMonitorCommand(props);

        echoLocal("Killing %s Agents", addresses.size());
        for (AgentAddress address : addresses) {
            echoLocal("Killing Agent, %s", address.publicAddress);
            bash.killAllJavaProcesses(address.publicAddress);
            if (startHarakiriMonitorCommand != null) {
                bash.ssh(address.publicAddress, startHarakiriMonitorCommand);
            }
        }
        echoLocal("Successfully killed %s Agents", addresses.size());
    }

    private void initMemberWorkerCount(WorkerJvmSettings masterSettings) {
        int agentCount = agentsClient.getAgentCount();
        if (masterSettings.memberWorkerCount == -1) {
            masterSettings.memberWorkerCount = agentCount;
        }
    }

    private void initHzConfig(WorkerJvmSettings settings) {
        String addressConfig = createAddressConfig("member", agentsClient.getPrivateAddresses(), settings);
        settings.hzConfig = settings.hzConfig.replace("<!--MEMBERS-->", addressConfig);

        String manCenterURL = props.get("MANAGEMENT_CENTER_URL").trim();
        if (!"none".equals(manCenterURL) && (manCenterURL.startsWith("http://") || manCenterURL.startsWith("https://"))) {
            String updateInterval = props.get("MANAGEMENT_CENTER_UPDATE_INTERVAL").trim();
            String updateIntervalAttr = (updateInterval.isEmpty()) ? "" : " update-interval=\"" + updateInterval + "\"";
            settings.hzConfig = settings.hzConfig.replace("<!--MANAGEMENT_CENTER_CONFIG-->",
                    format("<management-center enabled=\"true\"%s>%n        %s%n" + "    </management-center>%n",
                            updateIntervalAttr, manCenterURL));
        }
    }

    private void initClientHzConfig(WorkerJvmSettings settings) {
        String addressConfig = createAddressConfig("address", agentsClient.getPrivateAddresses(), settings);
        settings.clientHzConfig = settings.clientHzConfig.replace("<!--MEMBERS-->", addressConfig);
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
        if (!"yourkit".equals(workerJvmSettings.profiler)) {
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
        List<AgentMemberLayout> agentMemberLayouts = initMemberLayout();

        long started = System.nanoTime();
        try {
            echo("Killing all remaining workers");
            agentsClient.terminateWorkers();
            echo("Successfully killed all remaining workers");

            echo("Starting %d member workers", workerJvmSettings.memberWorkerCount);
            agentsClient.spawnWorkers(agentMemberLayouts, true);
            echo("Successfully started member workers");

            if (workerJvmSettings.clientWorkerCount > 0) {
                echo("Starting %d client workers", workerJvmSettings.clientWorkerCount);
                agentsClient.spawnWorkers(agentMemberLayouts, false);
                echo("Successfully started client workers");
            } else {
                echo("Skipping client startup, since no clients are configured");
            }
        } catch (Exception e) {
            throw new CommandLineExitException("Failed to start workers", e);
        }

        long durationMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
        LOGGER.info((format("Successfully started a grand total of %s Workers JVMs after %s ms",
                workerJvmSettings.totalWorkerCount(), durationMs)));
    }

    List<AgentMemberLayout> initMemberLayout() {
        int agentCount = agentsClient.getAgentCount();

        if (dedicatedMemberMachineCount > agentCount) {
            throw new CommandLineExitException(format("dedicatedMemberMachineCount %d can't be larger than number of agents %d",
                    dedicatedMemberMachineCount, agentCount));
        }
        if (workerJvmSettings.clientWorkerCount > 0 && agentCount - dedicatedMemberMachineCount < 1) {
            throw new CommandLineExitException("dedicatedMemberMachineCount is too big, there are no machines left for clients!");
        }

        List<AgentMemberLayout> agentMemberLayouts = initAgentMemberLayouts(agentsClient, workerJvmSettings);

        assignDedicatedMemberMachines(agentCount, agentMemberLayouts, dedicatedMemberMachineCount);

        AtomicInteger currentIndex = new AtomicInteger(0);
        for (int i = 0; i < workerJvmSettings.memberWorkerCount; i++) {
            // assign server nodes
            AgentMemberLayout agentLayout = findNextAgentLayout(currentIndex, agentMemberLayouts, AgentMemberMode.CLIENT);
            agentLayout.memberSettings.memberWorkerCount++;
        }
        for (int i = 0; i < workerJvmSettings.clientWorkerCount; i++) {
            // assign the clients
            AgentMemberLayout agentLayout = findNextAgentLayout(currentIndex, agentMemberLayouts, AgentMemberMode.MEMBER);
            agentLayout.clientSettings.clientWorkerCount++;
        }

        // log the layout
        for (AgentMemberLayout spawnPlan : agentMemberLayouts) {
            LOGGER.info(format("    Agent %s members: %d clients: %d mode: %s",
                    spawnPlan.publicIp,
                    spawnPlan.memberSettings.memberWorkerCount,
                    spawnPlan.clientSettings.clientWorkerCount,
                    spawnPlan.agentMemberMode
            ));
        }

        return agentMemberLayouts;
    }

    void runTestSuite() {
        echo("Starting testsuite: %s", testSuite.id);
        echo("Tests in testsuite: %s", testSuite.size());
        echo("Running time per test: %s ", secondsToHuman(testSuite.durationSeconds));
        echo("Expected total testsuite time: %s", secondsToHuman(testSuite.size() * testSuite.durationSeconds));

        long started = System.nanoTime();

        if (parallel) {
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
                        TestCaseRunner runner = new TestCaseRunner(testCase, testSuite, Coordinator.this, maxTestCaseIdLength,
                                testPhaseSyncMap);
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
            TestCaseRunner runner = new TestCaseRunner(testCase, testSuite, this, maxTestCaseIdLength, null);
            boolean success = runner.run();
            if (!success && testSuite.failFast) {
                LOGGER.info("Aborting testsuite due to failure");
                break;
            }
            if (!success || workerJvmSettings.refreshJvm) {
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
            if (testPhase == lastTestPhaseToSync) {
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

    private void echoLocal(String msg, Object... args) {
        echoLocal(format(msg, args));
    }

    private void echoLocal(String msg) {
        LOGGER.info(msg);
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

            Coordinator coordinator = new Coordinator();
            CoordinatorCli cli = new CoordinatorCli(coordinator, args);
            cli.init();

            LOGGER.info(format("Loading agents file: %s", coordinator.agentsFile.getAbsolutePath()));
            LOGGER.info(format("HAZELCAST_VERSION_SPEC: %s", coordinator.props.getHazelcastVersionSpec()));

            coordinator.run();
        } catch (Exception e) {
            exitWithError(LOGGER, "Failed to run testsuite", e);
        }
    }
}
