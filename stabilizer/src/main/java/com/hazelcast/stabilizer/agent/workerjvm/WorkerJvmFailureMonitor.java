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

    public void stop() {
        detectThread.interrupt();
        try {
            detectThread.join();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void detect() {
        WorkerJvmManager workerJvmManager = agent.getWorkerJvmManager();

        for (WorkerJvm jvm : workerJvmManager.getWorkerJvms()) {

            List<Failure> failures = new LinkedList<Failure>();

            addIfNotNull(failures, detectOomeFailure(jvm));

            addIfNotNull(failures, detectUnexpectedExit(jvm));

            if (!failures.isEmpty()) {
                workerJvmManager.terminateWorker(jvm);

                for (Failure failure : failures) {
                    publish(failure);
                }
            }
        }

        detectExceptions();
    }

    private void addIfNotNull(List<Failure> failures, Failure failure) {
        if (failure != null) {
            failures.add(failure);
        }
    }

    private void detectExceptions() {
        File testsuiteHome = agent.getTestSuiteDir();
        if (testsuiteHome == null) {
            return;
        }

        File[] files = testsuiteHome.listFiles();
        if (files == null) {
            return;
        }

        for (File file : files) {
            String name = file.getName();
            if (name.endsWith(".failure")) {
                String cause = Utils.asText(file);
                //we rename it so that we don't detect the same failure again.
                file.renameTo(new File(file.getAbsolutePath() + ".done"));

                String workerId = name.substring(0, name.lastIndexOf('@'));
                log.info("workerId: " + workerId);

                WorkerJvm jvm = agent.getWorkerJvmManager().getWorker(workerId);
                log.info("found worker: "+jvm);
                //todo: remove me
                log.info("Available jvms:"+agent.getWorkerJvmManager().workerJvms);

                Failure failure = new Failure();
                failure.message = "Exception thrown in worker";
                failure.agentAddress = getHostAddress();
                failure.workerAddress = jvm == null ? null : jvm.memberAddress;
                failure.workerId = workerId;
                failure.testCase = agent.getTestCase();
                failure.cause = cause;
                publish(failure);

                if (jvm != null) {
                    agent.getWorkerJvmManager().terminateWorker(jvm);
                }
            }
        }
    }

    private Failure detectOomeFailure(WorkerJvm jvm) {
        File testsuiteDir = agent.getTestSuiteDir();
        if (testsuiteDir == null) {
            return null;
        }

        File file = new File(testsuiteDir, jvm.id + ".oome");
        if (!file.exists()) {
            return null;
        }

        Failure failure = new Failure();
        failure.message = "Out of memory";
        failure.agentAddress = getHostAddress();
        failure.workerAddress = jvm.memberAddress;
        failure.workerId = jvm.id;
        failure.testCase = agent.getTestCase();
        jvm.process.destroy();
        return failure;
    }

    private Failure detectUnexpectedExit(WorkerJvm jvm) {
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
                return failure;
            }
        } catch (IllegalThreadStateException ignore) {
        }
        return null;
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
