/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.simulator.agent.workerjvm;

import com.hazelcast.simulator.agent.Agent;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.test.Failure;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FilenameFilter;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.simulator.utils.CommonUtils.sleepSeconds;
import static com.hazelcast.simulator.utils.FileUtils.deleteQuiet;
import static com.hazelcast.simulator.utils.FileUtils.fileAsText;
import static java.lang.String.format;

public class WorkerJvmFailureMonitor {

    private static final long LAST_SEEN_TIMEOUT_MILLIS = TimeUnit.SECONDS.toMillis(60);

    private static final Logger LOGGER = Logger.getLogger(WorkerJvmFailureMonitor.class);

    private final BlockingQueue<Failure> failureQueue = new LinkedBlockingQueue<Failure>();

    private final MonitorThread monitorThread;

    public WorkerJvmFailureMonitor(Agent agent, ConcurrentMap<SimulatorAddress, WorkerJvm> workerJVMs) {
        monitorThread = new MonitorThread(agent, workerJVMs);
        monitorThread.start();
    }

    public void shutdown() {
        monitorThread.running = false;
        monitorThread.interrupt();
    }

    public void drainFailures(List<Failure> failures) {
        failureQueue.drainTo(failures);
    }

    private class MonitorThread extends Thread {

        private final Agent agent;
        private final ConcurrentMap<SimulatorAddress, WorkerJvm> workerJVMs;

        private volatile boolean running = true;

        public MonitorThread(Agent agent, ConcurrentMap<SimulatorAddress, WorkerJvm> workerJVMs) {
            super("WorkerJvmFailureMonitorThread");
            setDaemon(true);

            this.agent = agent;
            this.workerJVMs = workerJVMs;
        }

        public void run() {
            while (running) {
                try {
                    detect();
                } catch (Exception e) {
                    LOGGER.fatal("Failed to scan for failures", e);
                }
                sleepSeconds(1);
            }
        }

        private void detect() {
            for (WorkerJvm jvm : workerJVMs.values()) {
                detectFailuresInJvm(jvm);
            }
        }

        private void detectFailuresInJvm(WorkerJvm jvm) {
            List<Failure> failures = new LinkedList<Failure>();

            detectOomeFailure(jvm, failures);
            detectExceptions(jvm, failures);
            detectInactivity(jvm, failures);
            detectUnexpectedExit(jvm, failures);

            for (Failure failure : failures) {
                LOGGER.warn("Failure detected: " + failure);
                failureQueue.add(failure);
            }
        }

        private void detectOomeFailure(WorkerJvm jvm, List<Failure> failures) {
            // once the failure is detected, we don't need to detect it again
            if (jvm.isOomeDetected()) {
                return;
            }

            if (!isOomeFound(jvm)) {
                return;
            }
            jvm.setOomeDetected();

            Failure failure = new Failure();
            failure.message = "Worker ran into an OOME";
            failure.type = Failure.Type.WORKER_OOM;
            failure.agentAddress = agent.getPublicAddress();
            failure.hzAddress = jvm.getHazelcastAddress();
            failure.workerId = jvm.getId();
            failure.testSuite = agent.getTestSuite();
            failures.add(failure);
        }

        private boolean isOomeFound(WorkerJvm jvm) {
            File oomeFile = new File(jvm.getWorkerHome(), "worker.oome");
            if (oomeFile.exists()) {
                return true;
            }

            // if we find the hprof file, we also know there is an OOME. The problem with the worker.oome file is that it is
            // created after the heap dump is done, and creating the heap dump can take a lot of time. And then the system could
            // think there is another problem (e.g. lack of inactivity; or timeouts). This hides the OOME.
            String[] hprofFiles = jvm.getWorkerHome().list(new HProfExtensionFilter());
            if (hprofFiles == null) {
                return false;
            }
            return (hprofFiles.length > 0);
        }

        private void detectExceptions(WorkerJvm workerJvm, List<Failure> failures) {
            File workerHome = workerJvm.getWorkerHome();
            if (!workerHome.exists()) {
                return;
            }

            File[] exceptionFiles = workerHome.listFiles(new ExceptionExtensionFilter());

            if (exceptionFiles == null) {
                return;
            }

            for (File exceptionFile : exceptionFiles) {
                String content = fileAsText(exceptionFile);

                int indexOf = content.indexOf('\n');
                String testId = content.substring(0, indexOf);
                String cause = content.substring(indexOf + 1);

                // we rename it so that we don't detect the same exception again
                deleteQuiet(exceptionFile);

                Failure failure = new Failure();
                failure.message = "Worked ran into an unhandled exception";
                failure.type = Failure.Type.WORKER_EXCEPTION;
                failure.agentAddress = agent.getPublicAddress();
                failure.hzAddress = workerJvm.getHazelcastAddress();
                failure.workerId = workerJvm.getId();
                failure.testId = testId;
                failure.testSuite = agent.getTestSuite();
                failure.cause = cause;
                failures.add(failure);
            }
        }

        private void detectInactivity(WorkerJvm jvm, List<Failure> failures) {
            long now = System.currentTimeMillis();

            if (jvm.isOomeDetected()) {
                return;
            }

            if (now - LAST_SEEN_TIMEOUT_MILLIS > jvm.getLastSeen()) {
                Failure failure = new Failure();
                failure.message = "Worker has not contacted agent for a too long period";
                failure.type = Failure.Type.WORKER_TIMEOUT;
                failure.agentAddress = agent.getPublicAddress();
                failure.hzAddress = jvm.getHazelcastAddress();
                failure.workerId = jvm.getId();
                failure.testSuite = agent.getTestSuite();
                failures.add(failure);
            }
        }

        private void detectUnexpectedExit(WorkerJvm jvm, List<Failure> failures) {
            if (jvm.isOomeDetected()) {
                return;
            }

            Process process = jvm.getProcess();
            int exitCode;
            try {
                exitCode = process.exitValue();
            } catch (IllegalThreadStateException ignore) {
                // process is still running
                return;
            }

            if (exitCode == 0) {
                return;
            }

            agent.terminateWorkerJvm(jvm);
            workerJVMs.remove(jvm.getAddress());

            Failure failure = new Failure();
            failure.message = format("Worker terminated with exit code %d instead of 0", exitCode);
            failure.type = Failure.Type.WORKER_EXIT;
            failure.agentAddress = agent.getPublicAddress();
            failure.hzAddress = jvm.getHazelcastAddress();
            failure.workerId = jvm.getId();
            failure.testSuite = agent.getTestSuite();
            failures.add(failure);
        }
    }

    private static class HProfExtensionFilter implements FilenameFilter {

        @Override
        public boolean accept(File dir, String name) {
            return name.endsWith(".hprof");
        }
    }

    private static class ExceptionExtensionFilter implements FilenameFilter {

        @Override
        public boolean accept(File dir, String name) {
            return name.endsWith(".exception");
        }
    }
}
