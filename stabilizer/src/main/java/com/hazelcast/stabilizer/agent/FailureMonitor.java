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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.Failure;

import java.io.File;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static com.hazelcast.stabilizer.Utils.getHostAddress;
import static com.hazelcast.stabilizer.Utils.readObject;
import static com.hazelcast.stabilizer.Utils.sleepSeconds;
import static com.hazelcast.stabilizer.Utils.throwableToString;

public class FailureMonitor {
    final static ILogger log = Logger.getLogger(FailureMonitor.class);

    private final Agent agent;
    private final BlockingQueue<Failure> failureQueue = new LinkedBlockingQueue<Failure>();

    public FailureMonitor(Agent agent) {
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
        new DetectThread().start();
    }

    private void addIfNotNull(List<Failure> failures, Failure h) {
        if (h != null) {
            failures.add(h);
        }
    }

    private void detect() {
        final WorkerJvmManager workerJvmManager = agent.getWorkerJvmManager();

        for (WorkerJvm jvm : workerJvmManager.getWorkerJvms()) {

            List<Failure> failures = new LinkedList<Failure>();

            addIfNotNull(failures, detectOomeFailure(jvm));

            addIfNotNull(failures, detectUnexpectedExit(jvm));

            addIfNotNull(failures, detectMembershipFailure(jvm));

            if (!failures.isEmpty()) {
                workerJvmManager.destroy(jvm);

                for (Failure failure : failures) {
                    publish(failure);
                }
            }
        }

        detectExceptions(workerJvmManager);
    }

    private void detectExceptions(WorkerJvmManager workerJvmManager) {
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
                WorkerJvm jvm = workerJvmManager.getWorker(workerId);

                Failure failure = new Failure();
                failure.message = "Exception thrown in worker";
                failure.agentAddress = getHostAddress();
                failure.workerAddress = jvm == null ? null : jvm.getMember().getInetSocketAddress().getHostString();
                failure.workerId = workerId;
                failure.testRecipe = agent.getTestRecipe();
                failure.cause = throwableToString(cause);
                publish(failure);

                if (jvm != null) {
                    workerJvmManager.destroy(jvm);
                }
            }
        }
    }

    private Failure detectMembershipFailure(WorkerJvm jvm) {
        //if the jvm is not assigned a hazelcast address yet.
        if (jvm.getMember() == null) {
            return null;
        }

        Member member = findMember(jvm);
        if (member == null) {
            jvm.getProcess().destroy();
            Failure failure = new Failure();
            failure.message = "Hazelcast membership failure (member missing)";
            failure.agentAddress = getHostAddress();
            failure.workerAddress = jvm.getMember().getInetSocketAddress().getHostString();
            failure.workerId = jvm.getId();
            failure.testRecipe = agent.getTestRecipe();
            return failure;
        }

        return null;
    }

    private Member findMember(WorkerJvm jvm) {
        final HazelcastInstance workerClient = agent.getWorkerJvmManager().getWorkerClient();
        if (workerClient == null) {
            return null;
        }

        for (Member member : workerClient.getCluster().getMembers()) {
            if (member.getInetSocketAddress().equals(jvm.getMember().getInetSocketAddress())) {
                return member;
            }
        }

        return null;
    }

    private Failure detectOomeFailure(WorkerJvm jvm) {
        File workoutDir = agent.getWorkoutHome();
        if (workoutDir == null) {
            return null;
        }

        File file = new File(workoutDir, jvm.getId() + ".oome");
        if (!file.exists()) {
            return null;
        }

        Failure failure = new Failure();
        failure.message = "Out of memory";
        failure.agentAddress = getHostAddress();
        failure.workerAddress = jvm.getMember().getInetSocketAddress().getHostString();
        failure.workerId = jvm.getId();
        failure.testRecipe = agent.getTestRecipe();
        jvm.getProcess().destroy();
        return failure;
    }

    private Failure detectUnexpectedExit(WorkerJvm jvm) {
        Process process = jvm.getProcess();
        try {
            int exitCode = process.exitValue();
            if (exitCode != 0) {
                Failure failure = new Failure();
                failure.message = "Exit code not 0, but was " + exitCode;
                failure.agentAddress = getHostAddress();
                failure.workerAddress = jvm.getMember().getInetSocketAddress().getHostString();
                failure.workerId = jvm.getId();
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
