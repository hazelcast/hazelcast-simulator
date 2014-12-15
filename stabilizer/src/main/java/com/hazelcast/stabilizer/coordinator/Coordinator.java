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
package com.hazelcast.stabilizer.coordinator;

import com.hazelcast.config.Config;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.TestCase;
import com.hazelcast.stabilizer.Utils;
import com.hazelcast.stabilizer.agent.SpawnWorkerFailedException;
import com.hazelcast.stabilizer.agent.workerjvm.WorkerJvmSettings;
import com.hazelcast.stabilizer.common.AgentAddress;
import com.hazelcast.stabilizer.common.AgentsFile;
import com.hazelcast.stabilizer.common.GitInfo;
import com.hazelcast.stabilizer.common.StabilizerProperties;
import com.hazelcast.stabilizer.coordinator.remoting.AgentsClient;
import com.hazelcast.stabilizer.provisioner.Bash;
import com.hazelcast.stabilizer.tests.Failure;
import com.hazelcast.stabilizer.tests.TestSuite;

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

import static com.hazelcast.stabilizer.Utils.getStablizerHome;
import static com.hazelcast.stabilizer.Utils.getVersion;
import static com.hazelcast.stabilizer.Utils.secondsToHuman;
import static java.lang.String.format;

public class Coordinator {

    public final static File STABILIZER_HOME = getStablizerHome();
    private final static ILogger log = Logger.getLogger(Coordinator.class);

    //options.
    public boolean monitorPerformance;
    public boolean verifyEnabled = true;
    public String workerClassPath;
    public Integer testStopTimeoutMs;
    public File agentsFile;
    public TestSuite testSuite;
    public int dedicatedMemberMachineCount;
    public boolean parallel;

    //internal state.
    final BlockingQueue<Failure> failureList = new LinkedBlockingQueue<Failure>();
    protected AgentsClient agentsClient;
    public WorkerJvmSettings workerJvmSettings;
    public StabilizerProperties props = new StabilizerProperties();
    public volatile double performance;
    public volatile long operationCount;
    private Bash bash;
    public PerformanceMonitor performanceMonitor;

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
            log.info("-----------------------------------------------------------------------------");
            log.info("No failures have been detected!");
            log.info("-----------------------------------------------------------------------------");
            System.exit(0);
        } else {
            log.info("-----------------------------------------------------------------------------");
            log.info(failureList.size() + " failures have been detected!!!!");
            log.info("-----------------------------------------------------------------------------");
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
        log.info(format("Performance monitor enabled: %s", monitorPerformance));
        log.info(format("Total number of agents: %s", agentCount));
        log.info(format("Total number of Hazelcast member workers: %s", workerJvmSettings.memberWorkerCount));
        log.info(format("Total number of Hazelcast client workers: %s", workerJvmSettings.clientWorkerCount));

        agentsClient.initTestSuite(testSuite);

        uploadWorkerClassPath();
        //todo: copy the hazelcast jars
        uploadYourKitIfNeeded();
    }

    private List<File> createUpload() throws IOException {
        return Utils.getFilesFromClassPath(workerClassPath);
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
        log.info("Starting cool down (10 sec)");
        Utils.sleepSeconds(10);
        log.info("Finished cool down");

        long elapsedMs = System.currentTimeMillis() - startMs;
        log.info(format("Total running time: %s seconds", elapsedMs / 1000));
    }

    private void runSequential() throws Exception {
        echo(format("Running %s tests sequentially", testSuite.size()));

        int maxTestCaseIdLength = getMaxTestCaseIdLength(testSuite.testCaseList);

        for (TestCase testCase : testSuite.testCaseList) {
            TestCaseRunner runner = new TestCaseRunner(testCase, testSuite, this, maxTestCaseIdLength);
            boolean success = runner.run();
            if (!success && testSuite.failFast) {
                log.info("Aborting testsuite due to failure");
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
                            log.info("Aborting testsuite due to failure");
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
            log.severe(e.getMessage());
            System.exit(1);
        }

        long durationMs = System.currentTimeMillis() - startMs;
        log.info((format("Successfully started a grand total of %s Workers JVM's after %s ms",
                workerJvmSettings.totalWorkerCount(), durationMs)));

        return startMs;
    }

    private List<AgentMemberLayout> initMemberLayout() {
        int agentCount = agentsClient.getAgentCount();

        if (dedicatedMemberMachineCount > agentCount) {
            Utils.exitWithError(log, "dedicatedMemberMachineCount can't be larger than number of agents. " +
                    "dedicatedMemberMachineCount is " + dedicatedMemberMachineCount + ", number of agents is: " + agentCount);
        }

        if (workerJvmSettings.clientWorkerCount > 0) {
            if (dedicatedMemberMachineCount > agentCount - 1) {
                Utils.exitWithError(log, "dedicatedMemberMachineCount is too big. There are no machines left for clients.");
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
            log.info("    Agent " + spawnPlan.publicIp
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
            log.warning("Failed to send echo message to agents due to timeout");
        }
        log.info(msg);
    }

    private void uploadWorkerClassPath() throws IOException {
        if (workerClassPath != null) {
            log.info(format("Copying workerClasspath '%s' to agents", workerClassPath));

            List<File> upload = createUpload();

            for (String ip : agentsClient.getPublicAddresses()) {
                for (File file : upload) {
                    String syncCommand =
                            format("rsync --ignore-existing -av -e \"ssh %s\" %s %s@%s:hazelcast-stabilizer-%s/workers/%s/lib",
                                    props.get("SSH_OPTIONS", ""),
                                    file.getAbsolutePath(),
                                    props.get("USER"),
                                    ip,
                                    getVersion(),
                                    testSuite.id);

                    bash.execute(syncCommand);
                }
                log.info("    " + ip + " copied");
            }

            log.info(format("Finished copying workerClasspath '%s' to agents", workerClassPath));
        }
    }

    private void uploadYourKitIfNeeded() {
        if ("yourkit".equals(workerJvmSettings.profiler)) {
            log.info("Ensuring Yourkit dependencies available on remote machines");

            //todo: in the future we'll only upload the right yourkit library 32 vs 64
            for (String ip : agentsClient.getPublicAddresses()) {
                bash.ssh(ip, format("mkdir -p hazelcast-stabilizer-%s/yourkit", getVersion()));

                String syncCommand = format("rsync --ignore-existing -av -e \"ssh %s\" %s/yourkit %s@%s:hazelcast-stabilizer-%s/",
                        props.get("SSH_OPTIONS", ""), getStablizerHome().getAbsolutePath(), props.get("USER"), ip, getVersion());

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
        log.info("Hazelcast Stabilizer Coordinator");
        log.info(format("Version: %s, Commit: %s, Build Time: %s",
                getVersion(), GitInfo.getCommitIdAbbrev(), GitInfo.getBuildTime()));
        log.info(format("STABILIZER_HOME: %s", STABILIZER_HOME));

        Coordinator coordinator = new Coordinator();
        CoordinatorCli cli = new CoordinatorCli(coordinator);
        cli.init(args);

        log.info(format("Loading agents file: %s", coordinator.agentsFile.getAbsolutePath()));
        log.info(format("HAZELCAST_VERSION_SPEC: %s", coordinator.props.getHazelcastVersionSpec()));

        try {
            coordinator.run();
            System.exit(0);
        } catch (Exception e) {
            log.severe("Failed to run testsuite", e);
            System.exit(1);
        }
    }
}
