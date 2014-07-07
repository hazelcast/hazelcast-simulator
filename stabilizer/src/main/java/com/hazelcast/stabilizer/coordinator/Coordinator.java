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
import com.hazelcast.stabilizer.common.StabilizerProperties;
import com.hazelcast.stabilizer.provisioner.Bash;
import com.hazelcast.stabilizer.tests.Failure;
import com.hazelcast.stabilizer.tests.TestSuite;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

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

    //internal state.
    final BlockingQueue<Failure> failureList = new LinkedBlockingQueue<Failure>();
    protected AgentsClient agentsClient;
    public WorkerJvmSettings workerJvmSettings;
    public StabilizerProperties props = new StabilizerProperties();
    public volatile double performance;
    public volatile long operationCount;
    private Bash bash;
    public boolean parallel;
    public boolean sslConnection;

    private void run() throws Exception {
        bash = new Bash(props);

        initAgents();

        startWorkers();

        new FailureMonitorThread(this).start();

        if (monitorPerformance) {
            new PerformanceMonitor(this).start();
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
        agentsClient = new AgentsClient(agentsFile);
        agentsClient.awaitAgentsReachable();

        initMemberWorkerCount(workerJvmSettings);
        initHzConfig(workerJvmSettings);
        initClientHzConfig(workerJvmSettings);

        int agentCount = agentsClient.getAgentCount();
        log.info(format("Performance monitor enabled: %s", monitorPerformance));
        log.info(format("Total number of agents: %s", agentCount));
        log.info(format("Total number of Hazelcast member workers: %s", workerJvmSettings.memberWorkerCount));
        log.info(format("Total number of Hazelcast client workers: %s", workerJvmSettings.clientWorkerCount));
        log.info(format("Total number of Hazelcast mixed client & member workers: %s", workerJvmSettings.mixedWorkerCount));

        agentsClient.initTestSuite(testSuite);

        if (workerClassPath != null) {
            log.info(format("Copying workerClasspath '%s' to agents", workerClassPath));

            List<File> upload = createUpload();

            for (String ip : agentsClient.getPublicAddresses()) {
                for (File file : upload) {
                    String syncCommand = format("rsync --ignore-existing -av -e \"ssh %s\" %s %s@%s:hazelcast-stabilizer-%s/workers/%s/lib",
                            props.get("SSH_OPTIONS", ""), file.getAbsolutePath(), props.get("USER"), ip, getVersion(), testSuite.id);

                    bash.execute(syncCommand);
                }
            }

            log.info(format("Finished copying workerClasspath '%s' to agents", workerClassPath));
        }

        //todo: copy the hazelcast jars

        //upload yourkit if needed
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

    public List<File> createUpload() throws IOException {
        if (workerClassPath == null) {
            return Collections.EMPTY_LIST;
        }

        List<File> files = new LinkedList<File>();
        for (String filePath : workerClassPath.split(";")) {
            File file = new File(filePath);

            if (file.getName().contains("*")) {
                File parent = file.getParentFile();
                if (!parent.isDirectory()) {
                    throw new IOException(format("Can't create upload, file [%s] is not a directory", parent));
                }

                String regex = file.getName().replace("*", "(.*)");
                for (File child : parent.listFiles()) {
                    if (child.getName().matches(regex)) {
                        files.add(child);
                    }
                }
            } else if (file.exists()) {
                files.add(file);
            } else {
                Utils.exitWithError(log, format("Can't create workerClassPath upload, file [%s] doesn't exist", filePath));
            }
        }

        return files;
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

        String home = "/home/users/stabilizer/hazelcast-stabilizer-0.3-SNAPSHOT";
        //String home = Utils.getStablizerHome().getAbsolutePath();

        String ssl = "<ssl enabled=\"true\">\n" +
                "            <factory-class-name>com.hazelcast.nio.ssl.BasicSSLContextFactory</factory-class-name>\n" +
                "            <properties>\n" +
                "                <property name=\"keyStore\">"+ home +"/conf/keystore_default.jks</property>\n" +
                "                <property name=\"keyStorePassword\">default</property>\n" +
                "                <property name=\"keyManagerAlgorithm\">SunX509</property>\n" +
                "                <property name=\"trustManagerAlgorithm\">SunX509</property>\n" +
                "                <property name=\"protocol\">TLS</property>\n" +
                "            </properties>\n" +
                "        </ssl>";

        String licenseKey = "<license-key>ABNKCFLH9DO3500101R8W0YR74Z00X</license-key>";

        settings.hzConfig = settings.hzConfig.replace("<!--MEMBERS-->", members);

        if(sslConnection){
            settings.hzConfig = settings.hzConfig.replace("<!--SSL-->", ssl);
            settings.hzConfig = settings.hzConfig.replace("<!--license-key-->", licenseKey);
        }
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

        String home = "/home/users/stabilizer/hazelcast-stabilizer-0.3-SNAPSHOT";
        //String home = Utils.getStablizerHome().getAbsolutePath();

        String ssl = "<ssl enabled=\"true\">\n" +
                "            <factory-class-name>com.hazelcast.nio.ssl.BasicSSLContextFactory</factory-class-name>\n" +
                "            <properties>\n" +
                "                <property name=\"keyStore\">"+ home +"/conf/keystore_default.jks</property>\n" +
                "                <property name=\"keyStorePassword\">default</property>\n" +
                "                <property name=\"keyManagerAlgorithm\">SunX509</property>\n" +
                "                <property name=\"trustManagerAlgorithm\">SunX509</property>\n" +
                "                <property name=\"protocol\">TLS</property>\n" +
                "            </properties>\n" +
                "        </ssl>";

        settings.clientHzConfig = settings.clientHzConfig.replace("<!--MEMBERS-->", members);

        if(sslConnection){
            settings.clientHzConfig = settings.clientHzConfig.replace("<!--SSL-->", ssl);
        }
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

        if(parallel){
            runParallel();
        }else{
            runSequential();
        }

        terminateWorkers();

        //the coordinator needs to sleep some to make sure that it will get failures if they are there.
        log.info("Starting cool down (20 sec)");
        Utils.sleepSeconds(20);
        log.info("Finished cool down");

        long elapsedMs = System.currentTimeMillis() - startMs;
        log.info(format("Total running time: %s seconds", elapsedMs / 1000));
    }

    private void runSequential() throws Exception {
        echo(format("Running %s tests sequentially",testSuite.size()));

        for (TestCase testCase : testSuite.testCaseList) {
            TestCaseRunner runner = new TestCaseRunner(testCase, testSuite, this);
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
        echo(format("Running %s tests parallel",testSuite.size()));

        ExecutorService executor = Executors.newFixedThreadPool(testSuite.size());

        List<Future> futures = new LinkedList<Future>();
        for (final TestCase testCase : testSuite.testCaseList) {
           Future f = executor.submit(new Runnable() {
               @Override
               public void run() {
                   try {
                       TestCaseRunner runner = new TestCaseRunner(testCase, testSuite, Coordinator.this);
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

        for(Future f: futures){
            f.get();
        }
    }

    void terminateWorkers() throws Exception {
        echo("Terminating workers");
        agentsClient.terminateWorkers();
        echo("All workers have been terminated");
    }

    long startWorkers() throws Exception {
        echo("Starting workers");

        long startMs = System.currentTimeMillis();

        int agentCount = agentsClient.getAgentCount();

        WorkerJvmSettings[] settingsArray = new WorkerJvmSettings[agentCount];
        for (int k = 0; k < agentCount; k++) {
            WorkerJvmSettings s = new WorkerJvmSettings(workerJvmSettings);
            s.memberWorkerCount = 0;
            s.mixedWorkerCount = 0;
            s.clientWorkerCount = 0;
            settingsArray[k] = s;
        }

        int index = 0;
        for (int k = 0; k < workerJvmSettings.memberWorkerCount; k++) {
            WorkerJvmSettings s = settingsArray[index % agentCount];
            s.memberWorkerCount++;
            index++;
        }
        for (int k = 0; k < workerJvmSettings.clientWorkerCount; k++) {
            WorkerJvmSettings s = settingsArray[index % agentCount];
            s.clientWorkerCount++;
            index++;
        }
        for (int k = 0; k < workerJvmSettings.mixedWorkerCount; k++) {
            WorkerJvmSettings s = settingsArray[index % agentCount];
            s.mixedWorkerCount++;
            index++;
        }

        try {
            agentsClient.spawnWorkers(settingsArray);
        } catch (SpawnWorkerFailedException e) {
            log.severe(e.getMessage());
            System.exit(1);
        }
        long durationMs = System.currentTimeMillis() - startMs;
        log.info((format("Finished starting a grand total of %s Workers JVM's after %s ms",
                workerJvmSettings.totalWorkerCount(), durationMs)));

        return startMs;
    }

    private void initMemberWorkerCount(WorkerJvmSettings masterSettings) {
        int agentCount = agentsClient.getAgentCount();
        if (masterSettings.memberWorkerCount == -1 && masterSettings.mixedWorkerCount == 0) {
            masterSettings.memberWorkerCount = agentCount;
        }
    }

    private void echo(String msg) {
        agentsClient.echo(msg);
        log.info(msg);
    }

    public static void main(String[] args) throws Exception {
        log.info("Hazelcast Stabilizer Coordinator");
        log.info(format("Version: %s", getVersion()));
        log.info(format("STABILIZER_HOME: %s", STABILIZER_HOME));

        Coordinator coordinator = new Coordinator();
        CoordinatorCli cli = new CoordinatorCli(coordinator);
        cli.init(args);

        log.info(format("Loading agents file: %s", coordinator.agentsFile.getAbsolutePath()));

        try {
            coordinator.run();
            System.exit(0);
        } catch (Exception e) {
            log.severe("Failed to run testsuite", e);
            System.exit(1);
        }
    }
}
