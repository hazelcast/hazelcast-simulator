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
import com.hazelcast.stabilizer.Utils;
import com.hazelcast.stabilizer.worker.WorkerJvm;
import com.hazelcast.stabilizer.worker.WorkerVmManager;

import java.io.File;
import java.util.LinkedList;
import java.util.List;

import static com.hazelcast.stabilizer.Utils.readObject;

public class FailureMonitor implements Runnable {
    final static ILogger log = Logger.getLogger(FailureMonitor.class);

    private Agent agent;

    public FailureMonitor(Agent agent) {
        this.agent = agent;
    }

    private void addIfNotNull(List<Failure> failures, Failure h) {
        if (h != null) {
            failures.add(h);
        }
    }

    public void run() {
        for (; ; ) {
            try {
                detect();
            } catch (Exception e) {
                log.severe("Failed to scan for failures", e);
            }
            Utils.sleepSeconds(1);
        }
    }

    private void detect() {
        final WorkerVmManager workerVmManager = agent.getWorkerVmManager();

        for (WorkerJvm jvm : workerVmManager.getWorkerJvms()) {

            List<Failure> failures = new LinkedList<Failure>();

            addIfNotNull(failures, detectOomeFailureFile(jvm));

            addIfNotNull(failures, detectUnexpectedExit(jvm));

            addIfNotNull(failures, detectMembershipFailure(jvm));

            if (!failures.isEmpty()) {
                workerVmManager.destroy(jvm);

                for (Failure failure : failures) {
                    agent.publishFailure(failure);
                }
            }
        }

        File workoutHome = agent.getWorkoutHome();
        if (workoutHome != null) {
            File[] files = workoutHome.listFiles();
            if (files != null) {
                for (File file : files) {
                    final String name = file.getName();
                    if (name.endsWith(".exception")) {
                        Throwable cause = (Throwable) readObject(file);
                        file.delete();

                        String workerId = name.substring(0, name.indexOf('.'));
                        log.info("workerId: " + workerId);
                        WorkerJvm jvm = workerVmManager.getWorker(workerId);
                        Failure failure = new Failure(
                                "Exception thrown in worker",
                                agent.getAgentHz().getCluster().getLocalMember().getInetSocketAddress(),
                                jvm == null ? null : jvm.getMember().getInetSocketAddress(),
                                workerId,
                                agent.getExerciseRecipe(),
                                cause);
                        agent.publishFailure(failure);
                        workerVmManager.destroy(jvm);
                    }
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
            return new Failure("Hazelcast membership failure (member missing)",
                    agent.getAgentHz().getCluster().getLocalMember().getInetSocketAddress(),
                    jvm.getMember().getInetSocketAddress(),
                    jvm.getId(),
                    agent.getExerciseRecipe());
        }

        return null;
    }

    private Member findMember(WorkerJvm jvm) {
        final HazelcastInstance workerClient = agent.getWorkerVmManager().getWorkerClient();
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

    private Failure detectOomeFailureFile(WorkerJvm jvm) {
        File workoutDir = agent.getWorkoutHome();
        if (workoutDir == null) {
            return null;
        }

        File file = new File(workoutDir, jvm.getId() + ".oome");
        if (!file.exists()) {
            return null;
        }

        Failure failure = new Failure(
                "out of memory",
                agent.getAgentHz().getCluster().getLocalMember().getInetSocketAddress(),
                jvm.getMember().getInetSocketAddress(),
                jvm.getId(),
                agent.getExerciseRecipe());
        jvm.getProcess().destroy();
        return failure;
    }

    private Failure detectUnexpectedExit(WorkerJvm jvm) {
        Process process = jvm.getProcess();
        try {
            if (process.exitValue() != 0) {
                return new Failure(
                        "exit code not 0",
                        agent.getAgentHz().getCluster().getLocalMember().getInetSocketAddress(),
                        jvm.getMember().getInetSocketAddress(),
                        jvm.getId(),
                        agent.getExerciseRecipe());
            }
        } catch (IllegalThreadStateException ignore) {
        }
        return null;
    }
}
