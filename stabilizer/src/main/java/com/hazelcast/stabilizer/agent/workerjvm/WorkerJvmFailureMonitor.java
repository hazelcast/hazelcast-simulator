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
package com.hazelcast.stabilizer.agent.workerjvm;


import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.Utils;
import com.hazelcast.stabilizer.agent.Agent;
import com.hazelcast.stabilizer.tests.Failure;

import java.io.File;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static com.hazelcast.stabilizer.Utils.getHostAddress;
import static com.hazelcast.stabilizer.Utils.sleepSeconds;

public class WorkerJvmFailureMonitor {
    final static ILogger log = Logger.getLogger(WorkerJvmFailureMonitor.class);

    private final Agent agent;
    private final BlockingQueue<Failure> failureQueue = new LinkedBlockingQueue<Failure>();
    private final DetectThread detectThread = new DetectThread();

    public WorkerJvmFailureMonitor(Agent agent) {
        this.agent = agent;
    }

    public void drainFailures(List<Failure> failures) {
        failureQueue.drainTo(failures);
    }

    public void publish(Failure failure) {
        log.severe("Failure detected: " + failure);
        failureQueue.add(failure);
    }

    public void start() {
        detectThread.start();
    }

    private void detect() {
        WorkerJvmManager workerJvmManager = agent.getWorkerJvmManager();

        for (WorkerJvm jvm : workerJvmManager.getWorkerJvms()) {

            List<Failure> failures = new LinkedList<Failure>();

            detectOomeFailure(jvm, failures);

            detectUnexpectedExit(jvm, failures);

            detectExceptions(jvm, failures);

            if (!failures.isEmpty()) {
                workerJvmManager.terminateWorker(jvm);

                for (Failure failure : failures) {
                    publish(failure);
                }
            }
        }
    }


    private void detectExceptions(WorkerJvm workerJvm, List<Failure> failures) {
        File workerHome = workerJvm.workerHome;
        if (workerHome == null) {
            return;
        }

        File[] files = workerHome.listFiles();
        if (files == null) {
            return;
        }

        for (File file : files) {
            String name = file.getName();
            if (name.endsWith(".exception")) {
                String cause = Utils.fileAsText(file);
                //we rename it so that we don't detect the same failure again.
                file.delete();
                Failure failure = new Failure();
                failure.message = "Exception thrown in worker";
                failure.agentAddress = getHostAddress();
                failure.workerAddress = workerJvm.memberAddress;
                failure.workerId = workerJvm.id;
                failure.testCase = agent.getTestCase();
                failure.cause = cause;
                failures.add(failure);
                agent.getWorkerJvmManager().terminateWorker(workerJvm);
            }
        }
    }

    private void detectOomeFailure(WorkerJvm jvm, List<Failure> failures) {
        File testsuiteDir = agent.getTestSuiteDir();
        if (testsuiteDir == null) {
            return;
        }

        File file = new File(testsuiteDir, "worker.oome");
        if (!file.exists()) {
            return;
        }

        Failure failure = new Failure();
        failure.message = "Out of memory";
        failure.agentAddress = getHostAddress();
        failure.workerAddress = jvm.memberAddress;
        failure.workerId = jvm.id;
        failure.testCase = agent.getTestCase();
        jvm.process.destroy();
        failures.add(failure);
    }

    private void detectUnexpectedExit(WorkerJvm jvm, List<Failure> failures) {
        Process process = jvm.process;
        try {
            int exitCode = process.exitValue();
            if (exitCode != 0) {
                Failure failure = new Failure();
                failure.message = "Exit code not 0, but was " + exitCode;
                failure.agentAddress = getHostAddress();
                failure.workerAddress = jvm.memberAddress;
                failure.workerId = jvm.id;
                failure.testCase = agent.getTestCase();
                failures.add(failure);
            }
        } catch (IllegalThreadStateException ignore) {
        }
    }

    private class DetectThread extends Thread {
        public DetectThread() {
            super("FailureMonitorThread");
        }

        public void run() {
            for (; ; ) {
                try {
                    detect();
                } catch (Exception e) {
                    log.severe("Failed to scan for failures", e);
                }
                sleepSeconds(1);
            }
        }
    }
}
