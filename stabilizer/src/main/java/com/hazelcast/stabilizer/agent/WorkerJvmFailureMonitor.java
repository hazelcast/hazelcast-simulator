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
package com.hazelcast.stabilizer.agent;


import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.tests.Failure;

import java.io.File;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static com.hazelcast.stabilizer.Utils.getHostAddress;
import static com.hazelcast.stabilizer.Utils.readObject;
import static com.hazelcast.stabilizer.Utils.sleepSeconds;
import static com.hazelcast.stabilizer.Utils.throwableToString;

public class WorkerJvmFailureMonitor {
    final static ILogger log = Logger.getLogger(WorkerJvmFailureMonitor.class);

    private final Agent agent;
    private final BlockingQueue<Failure> failureQueue = new LinkedBlockingQueue<Failure>();
    private volatile boolean stop = false;
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
        stop = true;
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

            addIfNotNull(failures, detectMembershipFailure(jvm));

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
        File workoutHome = agent.getWorkoutHome();
        if (workoutHome == null) {
            return;
        }

        File[] files = workoutHome.listFiles();
        if (files == null) {
            return;
        }

        for (File file : files) {
            String name = file.getName();
            if (name.endsWith(".exception")) {
                Throwable cause = (Throwable) readObject(file);
                file.delete();

                String workerId = name.substring(0, name.indexOf('.'));
                log.info("workerId: " + workerId);
                WorkerJvm jvm = agent.getWorkerJvmManager().getWorker(workerId);

                Failure failure = new Failure();
                failure.message = "Exception thrown in worker";
                failure.agentAddress = getHostAddress();
                failure.workerAddress = jvm == null ? null : jvm.getHostString();
                failure.workerId = workerId;
                failure.testRecipe = agent.getTestRecipe();
                failure.cause = throwableToString(cause);
                publish(failure);

                if (jvm != null) {
                    agent.getWorkerJvmManager().terminateWorker(jvm);
                }
            }
        }
    }

    private Failure detectMembershipFailure(WorkerJvm jvm) {
        Boolean isMember = agent.getWorkerJvmManager().isClusterMember(jvm);

        if (Boolean.FALSE.equals(isMember)) {
            jvm.process.destroy();
            Failure failure = new Failure();
            failure.message = "Hazelcast membership failure (member missing)";
            failure.agentAddress = getHostAddress();
            failure.workerAddress = jvm.getHostString();
            failure.workerId = jvm.id;
            failure.testRecipe = agent.getTestRecipe();
            return failure;
        }

        return null;
    }

    private Failure detectOomeFailure(WorkerJvm jvm) {
        File workoutDir = agent.getWorkoutHome();
        if (workoutDir == null) {
            return null;
        }

        File file = new File(workoutDir, jvm.id + ".oome");
        if (!file.exists()) {
            return null;
        }

        Failure failure = new Failure();
        failure.message = "Out of memory";
        failure.agentAddress = getHostAddress();
        failure.workerAddress = jvm.getHostString();
        failure.workerId = jvm.id;
        failure.testRecipe = agent.getTestRecipe();
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
                failure.workerAddress = jvm.getHostString();
                failure.workerId = jvm.id;
                failure.testRecipe = agent.getTestRecipe();
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
