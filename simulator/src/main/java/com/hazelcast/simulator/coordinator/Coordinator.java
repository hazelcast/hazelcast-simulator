/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.simulator.agent.SpawnWorkerFailedException;
import com.hazelcast.simulator.agent.workerjvm.WorkerJvmSettings;
import com.hazelcast.simulator.common.AgentAddress;
import com.hazelcast.simulator.common.AgentsFile;
import com.hazelcast.simulator.common.GitInfo;
import com.hazelcast.simulator.common.SimulatorProperties;
import com.hazelcast.simulator.coordinator.remoting.AgentsClient;
import com.hazelcast.simulator.provisioner.Bash;
import com.hazelcast.simulator.test.Failure;
import com.hazelcast.simulator.test.TestCase;
import com.hazelcast.simulator.test.TestSuite;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.simulator.coordinator.CoordinatorHelper.assignDedicatedMemberMachines;
import static com.hazelcast.simulator.coordinator.CoordinatorHelper.createAddressConfig;
import static com.hazelcast.simulator.coordinator.CoordinatorHelper.findNextAgentLayout;
import static com.hazelcast.simulator.coordinator.CoordinatorHelper.getMaxTestCaseIdLength;
import static com.hazelcast.simulator.coordinator.CoordinatorHelper.initAgentMemberLayouts;
import static com.hazelcast.simulator.utils.CommonUtils.exitWithError;
import static com.hazelcast.simulator.utils.CommonUtils.getSimulatorVersion;
import static com.hazelcast.simulator.utils.CommonUtils.secondsToHuman;
import static com.hazelcast.simulator.utils.CommonUtils.sleepSeconds;
import static com.hazelcast.simulator.utils.FileUtils.getFilesFromClassPath;
import static com.hazelcast.simulator.utils.FileUtils.getSimulatorHome;
import static java.lang.String.format;

public class Coordinator {

    public static final File SIMULATOR_HOME = getSimulatorHome();
    public static final File WORKING_DIRECTORY = new File(System.getProperty("user.dir"));
    public static final File UPLOAD_DIRECTORY = new File(WORKING_DIRECTORY, "upload");

    private static final Logger LOGGER = Logger.getLogger(Coordinator.class);

    // options
    public boolean monitorPerformance;
    public boolean verifyEnabled = true;
    public String workerClassPath;
    public Integer testStopTimeoutMs;
    public File agentsFile;
    public TestSuite testSuite;
    public int dedicatedMemberMachineCount;
    public boolean parallel;

    public WorkerJvmSettings workerJvmSettings;
    public volatile double performance;
    public volatile long operationCount;
    public PerformanceMonitor performanceMonitor;

    // internal state
    AgentsClient agentsClient;

    final BlockingQueue<Failure> failureList = new LinkedBlockingQueue<Failure>();
    final SimulatorProperties props = new SimulatorProperties();

    private final Bash bash = new Bash(props);

    private void run() throws Exception {
        initAgents();

        startWorkers();

        new FailureMonitorThread(this).start();

        if (monitorPerformance) {
            performanceMonitor = new PerformanceMonitor(this);
        }

        runTestSuite();

        logFailureInfo();
    }

    private void initAgents() throws Exception {
        List<AgentAddress> agentAddresses = AgentsFile.load(agentsFile);
        agentsClient = new AgentsClient(agentAddresses);
        agentsClient.start();

        initMemberWorkerCount(workerJvmSettings);
        initHzConfig(workerJvmSettings);
        initClientHzConfig(workerJvmSettings);

        int agentCount = agentsClient.getAgentCount();
        LOGGER.info(format("Performance monitor enabled: %s", monitorPerformance));
        LOGGER.info(format("Total number of agents: %s", agentCount));
        LOGGER.info(format("Total number of Hazelcast member workers: %s", workerJvmSettings.memberWorkerCount));
        LOGGER.info(format("Total number of Hazelcast client workers: %s", workerJvmSettings.clientWorkerCount));

        agentsClient.initTestSuite(testSuite);

        uploadUploadDirectory();
        uploadWorkerClassPath();
        uploadYourKitIfNeeded();
        //TODO: copy the hazelcast jars
    }

    private void initMemberWorkerCount(WorkerJvmSettings masterSettings) {
        int agentCount = agentsClient.getAgentCount();
        if (masterSettings.memberWorkerCount == -1) {
            masterSettings.memberWorkerCount = agentCount;
        }
    }

    private void initHzConfig(WorkerJvmSettings settings) throws Exception {
        String addressConfig = createAddressConfig("member", agentsClient.getPrivateAddresses(), settings);
        settings.hzConfig = settings.hzConfig.replace("<!--MEMBERS-->", addressConfig);
    }

    private void initClientHzConfig(WorkerJvmSettings settings) throws Exception {
        String addressConfig = createAddressConfig("address", agentsClient.getPrivateAddresses(), settings);
        settings.clientHzConfig = settings.clientHzConfig.replace("<!--MEMBERS-->", addressConfig);
    }

    private void uploadUploadDirectory() throws IOException {
        if (!UPLOAD_DIRECTORY.exists()) {
            LOGGER.debug("Skipping upload, since no upload file in working directory");
            return;
        }

        LOGGER.info(format("Starting uploading '+%s+' to agents", UPLOAD_DIRECTORY.getAbsolutePath()));
        List<File> files = getFilesFromClassPath(UPLOAD_DIRECTORY.getAbsolutePath());
        for (String ip : agentsClient.getPublicAddresses()) {
            LOGGER.info(format(" Uploading '+%s+' to agent %s", UPLOAD_DIRECTORY.getAbsolutePath(), ip));
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
        LOGGER.info(format("Finished uploading '+%s+' to agents", UPLOAD_DIRECTORY.getAbsolutePath()));
    }

    private void uploadWorkerClassPath() throws IOException {
        if (workerClassPath == null) {
            return;
        }

        LOGGER.info(format("Copying workerClasspath '%s' to agents", workerClassPath));
        List<File> upload = getFilesFromClassPath(workerClassPath);
        for (String ip : agentsClient.getPublicAddresses()) {
            for (File file : upload) {
                bash.execute(format("rsync --ignore-existing -avv -e \"ssh %s\" %s %s@%s:hazelcast-simulator-%s/workers/%s/lib",
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

    private void startWorkers() throws Exception {
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
        } catch (SpawnWorkerFailedException e) {
            exitWithError(LOGGER, "Failed to start workers", e);
        }

        long durationMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
        LOGGER.info((format("Successfully started a grand total of %s Workers JVMs after %s ms",
                workerJvmSettings.totalWorkerCount(), durationMs)));
    }

    List<AgentMemberLayout> initMemberLayout() {
        int agentCount = agentsClient.getAgentCount();

        if (dedicatedMemberMachineCount > agentCount) {
            exitWithError(LOGGER, format("dedicatedMemberMachineCount %d can't be larger than number of agents %d",
                    dedicatedMemberMachineCount, agentCount));
        }
        if (workerJvmSettings.clientWorkerCount > 0 && agentCount - dedicatedMemberMachineCount < 1) {
            exitWithError(LOGGER, "dedicatedMemberMachineCount is too big, there are no machines left for clients!");
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

    private void runTestSuite() throws Exception {
        echo("Starting testsuite: %s", testSuite.id);
        echo("Tests in testsuite: %s", testSuite.size());
        echo("Running time per test: %s ", secondsToHuman(testSuite.duration));
        echo("Expected total testsuite time: %s", secondsToHuman(testSuite.size() * testSuite.duration));

        long started = System.nanoTime();

        if (parallel) {
            runParallel();
        } else {
            runSequential();
        }

        terminateWorkers();

        // the coordinator needs to sleep some time to make sure that it will get failures if they are there
        LOGGER.info("Starting cool down (10 sec)");
        sleepSeconds(10);
        LOGGER.info("Finished cool down");

        long duration = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - started);
        LOGGER.info(format("Total running time: %s seconds", duration));
    }

    private void runParallel() throws InterruptedException, java.util.concurrent.ExecutionException {
        echo("Running %s tests parallel", testSuite.size());

        final int maxTestCaseIdLength = getMaxTestCaseIdLength(testSuite.testCaseList);
        ExecutorService executor = Executors.newFixedThreadPool(testSuite.size());

        List<Future> futures = new LinkedList<Future>();
        for (final TestCase testCase : testSuite.testCaseList) {
            Future future = executor.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        TestCaseRunner runner = new TestCaseRunner(testCase, testSuite, Coordinator.this, maxTestCaseIdLength);
                        boolean success = runner.run();
                        if (!success && testSuite.failFast) {
                            LOGGER.info("Aborting testsuite due to failure");
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            });
            futures.add(future);
        }
        for (Future future : futures) {
            future.get();
        }
    }

    private void runSequential() throws Exception {
        echo("Running %s tests sequentially", testSuite.size());

        int maxTestCaseIdLength = getMaxTestCaseIdLength(testSuite.testCaseList);

        for (TestCase testCase : testSuite.testCaseList) {
            TestCaseRunner runner = new TestCaseRunner(testCase, testSuite, this, maxTestCaseIdLength);
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

    private void terminateWorkers() throws Exception {
        echo("Terminating workers");
        agentsClient.terminateWorkers();
        echo("All workers have been terminated");
    }

    private void logFailureInfo() {
        if (!failureList.isEmpty()) {
            LOGGER.fatal("-----------------------------------------------------------------------------");
            LOGGER.fatal(failureList.size() + " failures have been detected!!!");
            LOGGER.fatal("-----------------------------------------------------------------------------");
            System.exit(1);
        }
        LOGGER.info("-----------------------------------------------------------------------------");
        LOGGER.info("No failures have been detected!");
        LOGGER.info("-----------------------------------------------------------------------------");
    }

    private void echo(String msg, Object... args) {
        echo(format(msg, args));
    }

    private void echo(String msg) {
        agentsClient.echo(msg);
        LOGGER.info(msg);
    }

    public static void main(String[] args) throws Exception {
        LOGGER.info("Hazelcast Simulator Coordinator");
        LOGGER.info(format("Version: %s, Commit: %s, Build Time: %s",
                getSimulatorVersion(), GitInfo.getCommitIdAbbrev(), GitInfo.getBuildTime()));
        LOGGER.info(format("SIMULATOR_HOME: %s", SIMULATOR_HOME));

        Coordinator coordinator = new Coordinator();
        CoordinatorCli cli = new CoordinatorCli(coordinator);
        cli.init(args);

        LOGGER.info(format("Loading agents file: %s", coordinator.agentsFile.getAbsolutePath()));
        LOGGER.info(format("HAZELCAST_VERSION_SPEC: %s", coordinator.props.getHazelcastVersionSpec()));

        try {
            coordinator.run();
        } catch (Exception e) {
            LOGGER.fatal("Failed to run testsuite", e);
            exitWithError(LOGGER, e.getMessage());
        }
    }
}
