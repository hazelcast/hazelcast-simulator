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

import com.hazelcast.config.Config;
import com.hazelcast.config.XmlConfigBuilder;
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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeoutException;

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

    //options.
    public boolean monitorPerformance;
    public boolean verifyEnabled = true;
    public String workerClassPath;
    public Integer testStopTimeoutMs;
    public File agentsFile;
    public TestSuite testSuite;
    public int dedicatedMemberMachineCount;
    public boolean parallel;

    public WorkerJvmSettings workerJvmSettings;
    public SimulatorProperties props = new SimulatorProperties();
    public volatile double performance;
    public volatile long operationCount;
    public PerformanceMonitor performanceMonitor;
    protected AgentsClient agentsClient;
    //internal state.
    final BlockingQueue<Failure> failureList = new LinkedBlockingQueue<Failure>();
    private Bash bash;

    private void run() throws Exception {
        bash = new Bash(props);

        initAgents();

        startWorkers();

        new FailureMonitorThread(this).start();

        if (monitorPerformance) {
            performanceMonitor = new PerformanceMonitor(this);
        }

        runTestSuite();

        logFailureInfo();
    }

    private void logFailureInfo() {
        if (failureList.isEmpty()) {
            LOGGER.info("-----------------------------------------------------------------------------");
            LOGGER.info("No failures have been detected!");
            LOGGER.info("-----------------------------------------------------------------------------");
            System.exit(0);
        } else {
            LOGGER.info("-----------------------------------------------------------------------------");
            LOGGER.info(failureList.size() + " failures have been detected!!!!");
            LOGGER.info("-----------------------------------------------------------------------------");
            System.exit(1);
        }
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

        copyUploadDirectoryToAgents();
        uploadWorkerClassPath();
        //todo: copy the hazelcast jars
        uploadYourKitIfNeeded();
    }

    private List<File> createUpload() throws IOException {
        return getFilesFromClassPath(workerClassPath);
    }

    private void initHzConfig(WorkerJvmSettings settings) throws Exception {
        int port = getPort(settings);

        StringBuffer members = new StringBuffer();
        for (String hostAddress : agentsClient.getPrivateAddresses()) {
            members.append("<member>")
                    .append(hostAddress)
                    .append(":").append(port)
                    .append("</member>\n");
        }

        settings.hzConfig = settings.hzConfig.replace("<!--MEMBERS-->", members);
    }

    private void initClientHzConfig(WorkerJvmSettings settings) throws Exception {
        int port = getPort(settings);

        StringBuffer members = new StringBuffer();
        for (String hostAddress : agentsClient.getPrivateAddresses()) {
            members.append("<address>")
                    .append(hostAddress)
                    .append(":").append(port)
                    .append("</address>\n");
        }

        settings.clientHzConfig = settings.clientHzConfig.replace("<!--MEMBERS-->", members);
    }

    private int getPort(WorkerJvmSettings settings) throws UnsupportedEncodingException {
        Config config = new XmlConfigBuilder(new ByteArrayInputStream(settings.hzConfig.getBytes("UTF-8"))).build();
        return config.getNetworkConfig().getPort();
    }

    private void runTestSuite() throws Exception {
        echo(format("Starting testsuite: %s", testSuite.id));
        echo(format("Tests in testsuite: %s", testSuite.size()));
        echo(format("Running time per test: %s ", secondsToHuman(testSuite.duration)));
        echo(format("Expected total testsuite time: %s", secondsToHuman(testSuite.size() * testSuite.duration)));

        long startMs = System.currentTimeMillis();

        if (parallel) {
            runParallel();
        } else {
            runSequential();
        }

        terminateWorkers();

        //the coordinator needs to sleep some to make sure that it will get failures if they are there.
        LOGGER.info("Starting cool down (10 sec)");
        sleepSeconds(10);
        LOGGER.info("Finished cool down");

        long elapsedMs = System.currentTimeMillis() - startMs;
        LOGGER.info(format("Total running time: %s seconds", elapsedMs / 1000));
    }

    private void runSequential() throws Exception {
        echo(format("Running %s tests sequentially", testSuite.size()));

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

    private void runParallel() throws InterruptedException, java.util.concurrent.ExecutionException {
        echo(format("Running %s tests parallel", testSuite.size()));

        ExecutorService executor = Executors.newFixedThreadPool(testSuite.size());

        final int maxTestCaseIdLength = getMaxTestCaseIdLength(testSuite.testCaseList);

        List<Future> futures = new LinkedList<Future>();
        for (final TestCase testCase : testSuite.testCaseList) {
            Future f = executor.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        TestCaseRunner runner = new TestCaseRunner(testCase, testSuite, Coordinator.this, maxTestCaseIdLength);
                        boolean success = runner.run();
                        if (!success && testSuite.failFast) {
                            LOGGER.info("Aborting testsuite due to failure");
                            return;
                        }

//                       if (!success || workerJvmSettings.refreshJvm) {
//                           terminateWorkers();
//                           startWorkers();
//                       }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            });

            futures.add(f);
        }

        for (Future f : futures) {
            f.get();
        }
    }

    private void terminateWorkers() throws Exception {
        echo("Terminating workers");
        agentsClient.terminateWorkers();
        echo("All workers have been terminated");
    }

    private long startWorkers() throws Exception {
        List<AgentMemberLayout> agentMemberLayouts = initMemberLayout();

        long startMs = System.currentTimeMillis();
        try {
            echo("Killing all remaining workers");
            agentsClient.terminateWorkers();
            echo("Successfully killed all remaining workers");

            echo("Starting " + workerJvmSettings.memberWorkerCount + " member workers");
            agentsClient.spawnWorkers(agentMemberLayouts, true);
            echo("Successfully started member workers");

            if (workerJvmSettings.clientWorkerCount > 0) {
                echo("Starting " + workerJvmSettings.clientWorkerCount + " client workers");
                agentsClient.spawnWorkers(agentMemberLayouts, false);
                echo("Successfully started client workers");
            } else {
                echo("Skipping client startup, since no clients are configured");
            }
        } catch (SpawnWorkerFailedException e) {
            LOGGER.fatal(e.getMessage());
            System.exit(1);
        }

        long durationMs = System.currentTimeMillis() - startMs;
        LOGGER.info((format("Successfully started a grand total of %s Workers JVMs after %s ms",
                workerJvmSettings.totalWorkerCount(), durationMs)));

        return startMs;
    }

    private List<AgentMemberLayout> initMemberLayout() {
        int agentCount = agentsClient.getAgentCount();

        if (dedicatedMemberMachineCount > agentCount) {
            exitWithError(LOGGER, "dedicatedMemberMachineCount can't be larger than number of agents. " +
                    "dedicatedMemberMachineCount is " + dedicatedMemberMachineCount + ", number of agents is: " + agentCount);
        }

        if (workerJvmSettings.clientWorkerCount > 0) {
            if (dedicatedMemberMachineCount > agentCount - 1) {
                exitWithError(LOGGER, "dedicatedMemberMachineCount is too big. There are no machines left for clients.");
            }
        }

        List<AgentMemberLayout> agentMemberLayouts = new LinkedList<AgentMemberLayout>();

        for (String agentIp : agentsClient.getPublicAddresses()) {
            AgentMemberLayout layout = new AgentMemberLayout(workerJvmSettings);
            layout.publicIp = agentIp;
            layout.agentMemberMode = AgentMemberMode.mixed;
            agentMemberLayouts.add(layout);
        }

        if (dedicatedMemberMachineCount > 0) {
            for (int k = 0; k < dedicatedMemberMachineCount; k++) {
                agentMemberLayouts.get(k).agentMemberMode = AgentMemberMode.member;
            }

            for (int k = dedicatedMemberMachineCount; k < agentCount; k++) {
                agentMemberLayouts.get(k).agentMemberMode = AgentMemberMode.client;
            }
        }

        // assign server nodes
        int i = -1;
        for (int k = 0; k < workerJvmSettings.memberWorkerCount; k++) {
            for (; ; ) {
                i++;
                int index = i % agentMemberLayouts.size();
                AgentMemberLayout agentLayout = agentMemberLayouts.get(index);
                if (agentLayout.agentMemberMode != AgentMemberMode.client) {
                    agentLayout.memberSettings.memberWorkerCount++;
                    break;
                }

            }
        }

        // assign the clients.
        for (int k = 0; k < workerJvmSettings.clientWorkerCount; k++) {
            for (; ; ) {
                i++;
                AgentMemberLayout agentLayout = agentMemberLayouts.get(i % agentMemberLayouts.size());
                if (agentLayout.agentMemberMode != AgentMemberMode.member) {
                    agentLayout.clientSettings.clientWorkerCount++;
                    break;
                }
            }
        }

        // log the layout
        for (int k = 0; k < agentCount; k++) {
            AgentMemberLayout spawnPlan = agentMemberLayouts.get(k);
            LOGGER.info("    Agent " + spawnPlan.publicIp
                    + " members: " + spawnPlan.memberSettings.memberWorkerCount
                    + " clients: " + spawnPlan.clientSettings.clientWorkerCount);
        }

        return agentMemberLayouts;
    }

    private void initMemberWorkerCount(WorkerJvmSettings masterSettings) {
        int agentCount = agentsClient.getAgentCount();
        if (masterSettings.memberWorkerCount == -1) {
            masterSettings.memberWorkerCount = agentCount;
        }
    }

    private void echo(String msg) {
        try {
            agentsClient.echo(msg);
        } catch (TimeoutException e) {
            LOGGER.warn("Failed to send echo message to agents due to timeout");
        }
        LOGGER.info(msg);
    }

    private void copyUploadDirectoryToAgents() throws IOException {
        if (!UPLOAD_DIRECTORY.exists()) {
            LOGGER.debug("Skipping upload, since no upload file in working directory");
            return;
        }
        LOGGER.info(format("Starting uploading '+%s+' to agents", UPLOAD_DIRECTORY.getAbsolutePath()));
        List<File> files = getFilesFromClassPath(UPLOAD_DIRECTORY.getAbsolutePath());
        for (String ip : agentsClient.getPublicAddresses()) {
            LOGGER.info(format(" Uploading '+%s+' to agent %s", UPLOAD_DIRECTORY.getAbsolutePath(), ip));
            for (File file : files) {
                String syncCommand = format("rsync -avv -e \"ssh %s\" %s %s@%s:hazelcast-simulator-%s/workers/%s/",
                        props.get("SSH_OPTIONS", ""),
                        file,
                        props.get("USER"),
                        ip,
                        getSimulatorVersion(),
                        testSuite.id);
                bash.execute(syncCommand);
            }
            LOGGER.info("    " + ip + " copied");
        }
        LOGGER.info(format("Finished uploading '+%s+' to agents", UPLOAD_DIRECTORY.getAbsolutePath()));
    }


    private void uploadWorkerClassPath() throws IOException {
        if (workerClassPath != null) {
            LOGGER.info(format("Copying workerClasspath '%s' to agents", workerClassPath));

            List<File> upload = createUpload();

            for (String ip : agentsClient.getPublicAddresses()) {
                for (File file : upload) {
                    String syncCommand =
                            format("rsync --ignore-existing -avv -e \"ssh %s\" %s %s@%s:hazelcast-simulator-%s/workers/%s/lib",
                                    props.get("SSH_OPTIONS", ""),
                                    file.getAbsolutePath(),
                                    props.get("USER"),
                                    ip,
                                    getSimulatorVersion(),
                                    testSuite.id);

                    bash.execute(syncCommand);
                }
                LOGGER.info("    " + ip + " copied");
            }

            LOGGER.info(format("Finished copying workerClasspath '%s' to agents", workerClassPath));
        }
    }

    private void uploadYourKitIfNeeded() {
        if ("yourkit".equals(workerJvmSettings.profiler)) {
            LOGGER.info("Ensuring YourKit dependencies available on remote machines");

            //todo: in the future we'll only upload the requested YourKit library (32 or 64 bit)
            for (String ip : agentsClient.getPublicAddresses()) {
                bash.ssh(ip, format("mkdir -p hazelcast-simulator-%s/yourkit", getSimulatorVersion()));

                String syncCommand = format("rsync --ignore-existing -avv -e \"ssh %s\" %s/yourkit %s@%s:hazelcast-simulator-%s/",
                        props.get("SSH_OPTIONS", ""), getSimulatorHome().getAbsolutePath(), props.get("USER"),
                        ip, getSimulatorVersion());

                bash.execute(syncCommand);
            }
        }
    }

    private int getMaxTestCaseIdLength(List<TestCase> testCaseList) {
        int maxLength = Integer.MIN_VALUE;
        for (TestCase testCase : testCaseList) {
            if (testCase.id != null && !testCase.id.isEmpty() && testCase.id.length() > maxLength) {
                maxLength = testCase.id.length();
            }
        }
        return (maxLength > 0) ? maxLength : 0;
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
            System.exit(0);
        } catch (Exception e) {
            LOGGER.fatal("Failed to run testsuite", e);
            System.exit(1);
        }
    }
}
