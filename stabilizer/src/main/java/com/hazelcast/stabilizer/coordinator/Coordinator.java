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

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.TestRecipe;
import com.hazelcast.stabilizer.Utils;
import com.hazelcast.stabilizer.agent.workerjvm.WorkerJvmSettings;
import com.hazelcast.stabilizer.performance.Performance;
import com.hazelcast.stabilizer.tests.Failure;
import com.hazelcast.stabilizer.tests.TestSuite;
import com.hazelcast.stabilizer.worker.testcommands.GenericTestCommand;
import com.hazelcast.stabilizer.worker.testcommands.InitTestCommand;
import com.hazelcast.stabilizer.worker.testcommands.StopTestCommand;

import java.io.File;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static com.hazelcast.stabilizer.Utils.getStablizerHome;
import static com.hazelcast.stabilizer.Utils.getVersion;
import static com.hazelcast.stabilizer.Utils.secondsToHuman;
import static com.hazelcast.stabilizer.coordinator.CoordinatorCli.init;
import static java.lang.String.format;

public class Coordinator {

    public final static File STABILIZER_HOME = getStablizerHome();
    private final static ILogger log = Logger.getLogger(Coordinator.class.getName());

    //options.
    public boolean monitorPerformance;
    public boolean verifyEnabled = true;
    public String workerClassPath;
    public boolean cleanWorkersHome;
    public Integer testStopTimeoutMs;
    public File machinesFile;
    public TestSuite testSuite;

    //internal state.
    private final BlockingQueue<Failure> failureList = new LinkedBlockingQueue<Failure>();
    private AgentClientManager agentClientManager;
    public WorkerJvmSettings workerJvmSettings;

    private void start() throws Exception {
        agentClientManager = new AgentClientManager(this, machinesFile);
        agentClientManager.getFailures();
        new FailureMonitorThread().start();

        if (cleanWorkersHome) {
            echo("Starting cleanup workers home");
            agentClientManager.cleanWorkersHome();
            echo("Finished cleanup workers home");
        }

        byte[] bytes = Utils.createUpload(workerClassPath);
        agentClientManager.initTestSuite(testSuite, bytes);

        initWorkerHzConfig(workerJvmSettings);

        int agentCount = agentClientManager.getAgentCount();
        log.info(format("Worker track logging: %s", workerJvmSettings.trackLogging));
        log.info(format("Total number of agents: %s", agentCount));
        log.info(format("Total number of Hazelcast Cluster Member workers: %s", workerJvmSettings.memberWorkerCount));
        log.info(format("Total number of Hazelcast Cluster Client workers: %s", workerJvmSettings.clientWorkerCount));
        log.info(format("Total number of Hazelcast Cluster Client & Member Workers: %s", workerJvmSettings.mixedWorkerCount));

        long startMs = System.currentTimeMillis();

        runTestSuite(testSuite);

        //the coordinator needs to sleep some to make sure that it will get failures if they are there.
        log.info("Starting cool down (20 sec)");
        Utils.sleepSeconds(20);
        log.info("Finished cool down");

        long elapsedMs = System.currentTimeMillis() - startMs;
        log.info(format("Total running time: %s seconds", elapsedMs / 1000));

        if (failureList.isEmpty()) {
            log.info("-----------------------------------------------------------------------------");
            log.info("No failures have been detected!");
            log.info("-----------------------------------------------------------------------------");
            System.exit(0);
        } else {
            StringBuilder sb = new StringBuilder();
            sb.append(failureList.size()).append(" Failures have been detected!!!\n");
            for (Failure failure : failureList) {
                sb.append("-----------------------------------------------------------------------------\n");
                sb.append(failure).append('\n');
            }
            sb.append("-----------------------------------------------------------------------------\n");
            log.severe(sb.toString());
            System.exit(1);
        }
    }

    private void initWorkerHzConfig(WorkerJvmSettings settings) {
        StringBuffer members = new StringBuffer();
        for (String hostAddress : agentClientManager.getHostAddresses()) {
            members.append("<member>").append(hostAddress).append(":5701").append("</member>\n");
        }

        settings.hzConfig = settings.hzConfig.replace("<!--MEMBERS-->", members);
    }

    private void runTestSuite(TestSuite testSuite) throws Exception {
        echo(format("Starting testsuite: %s", testSuite.id));
        echo(format("Tests in testsuite: %s", testSuite.size()));
        echo(format("Running time per test: %s ", secondsToHuman(testSuite.duration)));
        echo(format("Expected total testsuite time: %s", secondsToHuman(testSuite.size() * testSuite.duration)));

        //we need to make sure that before we launch, there are no workers running anymore.
        terminateWorkers();
        startWorkers(workerJvmSettings);

        for (TestRecipe testRecipe : testSuite.testRecipeList) {
            boolean success = run(testSuite, testRecipe);
            if (!success && testSuite.failFast) {
                log.info("Aborting working due to failure");
                break;
            }

            if (!success || workerJvmSettings.refreshJvm) {
                terminateWorkers();
                startWorkers(workerJvmSettings);
            }
        }

        terminateWorkers();
    }

    private boolean run(TestSuite testSuite, TestRecipe testRecipe) {
        echo(format("Running Test : %s", testRecipe.getTestId()));

        int oldCount = failureList.size();
        try {
            echo(testRecipe.toString());

            echo("Starting Test initialization");
            agentClientManager.prepareAgentsForTests(testRecipe);
            agentClientManager.executeOnAllWorkers(new InitTestCommand(testRecipe));
            echo("Completed Test initialization");

            echo("Starting Test local setup");
            agentClientManager.executeOnAllWorkers(new GenericTestCommand("localSetup"));
            echo("Completed Test local setup");

            echo("Starting Test global setup");
            agentClientManager.executeOnSingleWorker(new GenericTestCommand("globalSetup"));
            echo("Completed Test global setup");

            echo("Starting Test start");
            agentClientManager.executeOnAllWorkers(new GenericTestCommand("start"));
            echo("Completed Test start");

            echo(format("Test running for %s seconds", testSuite.duration));
            sleepSeconds(testSuite.duration);
            echo("Test finished running");

            echo("Starting Test stop");
            agentClientManager.executeOnAllWorkers(new StopTestCommand());
            echo("Completed Test stop");

            if (monitorPerformance) {
                echo(calcPerformance().toHumanString());
            }

            if (verifyEnabled) {
                echo("Starting Test global verify");
                agentClientManager.executeOnSingleWorker(new GenericTestCommand("globalVerify"));
                echo("Completed Test global verify");

                echo("Starting Test local verify");
                agentClientManager.executeOnAllWorkers(new GenericTestCommand("localVerify"));
                echo("Completed Test local verify");
            } else {
                echo("Skipping Test verification");
            }

            echo("Starting Test global tear down");
            agentClientManager.executeOnSingleWorker(new GenericTestCommand("globalTearDown"));
            echo("Finished Test global tear down");

            echo("Starting Test local tear down");
            agentClientManager.executeOnAllWorkers(new GenericTestCommand("localTearDown"));

            echo("Completed Test local tear down");

            return failureList.size() == oldCount;
        } catch (Exception e) {
            log.severe("Failed", e);
            return false;
        }
    }

    public void sleepSeconds(int seconds) {
        int period = 30;
        int big = seconds / period;
        int small = seconds % period;

        for (int k = 1; k <= big; k++) {
            if (failureList.size() > 0) {
                echo("Failure detected, aborting execution of test");
                return;
            }

            Utils.sleepSeconds(period);
            final int elapsed = period * k;
            final float percentage = (100f * elapsed) / seconds;
            String msg = format("Running %s of %s seconds %-4.2f percent complete", elapsed, seconds, percentage);
            echo(msg);
            if (monitorPerformance) {
                echo(calcPerformance().toHumanString());
            }
        }

        Utils.sleepSeconds(small);
    }

    public Performance calcPerformance() {
        return null;
//        ShoutToWorkersTask task = new ShoutToWorkersTask(new GenericTestTask("calcPerformance"), "calcPerformance");
//        Map<Member, Future<List<Performance>>> result = agentExecutor.submitToAllMembers(task);
//        Performance performance = null;
//        for (Future<List<Performance>> future : result.values()) {
//            try {
//                List<Performance> results = future.get();
//                for (Performance p : results) {
//                    if (performance == null) {
//                        performance = p;
//                    } else {
//                        performance = performance.merge(p);
//                    }
//                }
//            } catch (InterruptedException e) {
//            } catch (ExecutionException e) {
//                log.severe(e);
//            }
//        }
//        return performance == null ? new NotAvailable() : performance;
    }

    private void terminateWorkers() throws Exception {
        echo("Terminating workers");
        agentClientManager.terminateWorkers();
        echo("All workers have been terminated");
    }

    private long startWorkers(WorkerJvmSettings masterSettings) throws Exception {
        long startMs = System.currentTimeMillis();

        int agentCount = agentClientManager.getAgentCount();
        if (masterSettings.memberWorkerCount == -1) {
            masterSettings.memberWorkerCount = agentCount;
        }

        log.info(format("Starting %s Server Worker JVM's", masterSettings.memberWorkerCount));
        log.info(format("Starting %s Client Worker JVM's", masterSettings.clientWorkerCount));
        log.info(format("Starting %s Mixed Worker JVM's", masterSettings.mixedWorkerCount));

        WorkerJvmSettings[] settingsArray = new WorkerJvmSettings[agentCount];
        for (int k = 0; k < agentCount; k++) {
            WorkerJvmSettings s = new WorkerJvmSettings(masterSettings);
            s.memberWorkerCount = 0;
            s.mixedWorkerCount = 0;
            s.clientWorkerCount = 0;
            settingsArray[k] = s;
        }

        int index = 0;
        for (int k = 0; k < masterSettings.memberWorkerCount; k++) {
            WorkerJvmSettings s = settingsArray[index % agentCount];
            s.memberWorkerCount++;
            index++;
        }
        for (int k = 0; k < masterSettings.clientWorkerCount; k++) {
            WorkerJvmSettings s = settingsArray[index % agentCount];
            s.clientWorkerCount++;
            index++;
        }
        for (int k = 0; k < masterSettings.mixedWorkerCount; k++) {
            WorkerJvmSettings s = settingsArray[index % agentCount];
            s.mixedWorkerCount++;
            index++;
        }

        agentClientManager.spawnWorkers(settingsArray);

        long durationMs = System.currentTimeMillis() - startMs;
        log.info((format("Finished starting a grand total of %s Workers JVM's after %s ms\n",
                masterSettings.totalWorkerCount(), durationMs)));
        return startMs;
    }

    private void echo(String msg) {
        agentClientManager.echo(msg);
        log.info(msg);
    }

    public static void main(String[] args) throws Exception {
        log.info("Hazelcast Stabilizer Coordinator");
        log.info(format("Version: %s", getVersion()));
        log.info(format("STABILIZER_HOME: %s", STABILIZER_HOME));

        Coordinator coordinator = new Coordinator();
        init(coordinator, args);

        try {
            coordinator.start();
            System.exit(0);
        } catch (Exception e) {
            log.severe("Failed to run testsuite", e);
            System.exit(1);
        }
    }

    private class FailureMonitorThread extends Thread {
        public FailureMonitorThread() {
            super("FailureMonitorThread");
            setDaemon(true);
        }

        public void run() {
            for (; ; ) {
                //todo: this delay should be configurable.
                Utils.sleepSeconds(1);

                List<Failure> failures = agentClientManager.getFailures();
                for (Failure failure : failures) {
                    failureList.add(failure);
                    log.severe("Remote failure detected:" + failure);
                }
            }
        }
    }
}
