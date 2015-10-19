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
import com.hazelcast.simulator.protocol.core.Response;
import com.hazelcast.simulator.protocol.core.ResponseType;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.core.SimulatorProtocolException;
import com.hazelcast.simulator.protocol.operation.FailureOperation;
import com.hazelcast.simulator.test.FailureType;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FilenameFilter;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.simulator.test.FailureType.WORKER_EXCEPTION;
import static com.hazelcast.simulator.test.FailureType.WORKER_EXIT;
import static com.hazelcast.simulator.test.FailureType.WORKER_FINISHED;
import static com.hazelcast.simulator.test.FailureType.WORKER_OOM;
import static com.hazelcast.simulator.test.FailureType.WORKER_TIMEOUT;
import static com.hazelcast.simulator.utils.CommonUtils.sleepSeconds;
import static com.hazelcast.simulator.utils.FileUtils.deleteQuiet;
import static com.hazelcast.simulator.utils.FileUtils.fileAsText;
import static java.lang.String.format;

public class WorkerJvmFailureMonitor {

    private static final long LAST_SEEN_TIMEOUT_MILLIS = TimeUnit.SECONDS.toMillis(30);

    private static final Logger LOGGER = Logger.getLogger(WorkerJvmFailureMonitor.class);

    private final MonitorThread monitorThread;

    private int failureCount;

    public WorkerJvmFailureMonitor(Agent agent, ConcurrentMap<SimulatorAddress, WorkerJvm> workerJVMs) {
        monitorThread = new MonitorThread(agent, workerJVMs);
        monitorThread.start();
    }

    public void shutdown() {
        monitorThread.running = false;
        monitorThread.interrupt();
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
                    for (WorkerJvm workerJvm : workerJVMs.values()) {
                        detectFailures(workerJvm);
                    }
                } catch (Exception e) {
                    LOGGER.fatal("Failed to scan for failures", e);
                }
                sleepSeconds(1);
            }
        }

        private void detectFailures(WorkerJvm workerJvm) {
            if (workerJvm.isFinished()) {
                return;
            }
            detectExceptions(workerJvm);
            if (workerJvm.isOomeDetected()) {
                return;
            }
            detectOomeFailure(workerJvm);
            detectInactivity(workerJvm);
            detectUnexpectedExit(workerJvm);
        }

        private void detectExceptions(WorkerJvm workerJvm) {
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

                // we delete the exception file so that we don't detect the same exception again
                deleteQuiet(exceptionFile);

                sendFailureOperation("Worked ran into an unhandled exception", WORKER_EXCEPTION, workerJvm, testId, cause);
            }
        }

        private void detectOomeFailure(WorkerJvm workerJvm) {
            if (!isOomeFound(workerJvm)) {
                return;
            }
            workerJvm.setOomeDetected();

            sendFailureOperation("Worker ran into an OOME", WORKER_OOM, workerJvm);
        }

        private boolean isOomeFound(WorkerJvm workerJvm) {
            File oomeFile = new File(workerJvm.getWorkerHome(), "worker.oome");
            if (oomeFile.exists()) {
                return true;
            }

            // if we find the hprof file, we also know there is an OOME. The problem with the worker.oome file is that it is
            // created after the heap dump is done, and creating the heap dump can take a lot of time. And then the system could
            // think there is another problem (e.g. lack of inactivity; or timeouts). This hides the OOME.
            String[] hprofFiles = workerJvm.getWorkerHome().list(new HProfExtensionFilter());
            if (hprofFiles == null) {
                return false;
            }
            return (hprofFiles.length > 0);
        }

        private void detectInactivity(WorkerJvm workerJvm) {
            long elapsed = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - workerJvm.getLastSeen());
            if (elapsed > LAST_SEEN_TIMEOUT_MILLIS) {
                sendFailureOperation(format("Worker has not sent a message for %d seconds", elapsed), WORKER_TIMEOUT, workerJvm);
            }
        }

        private void detectUnexpectedExit(WorkerJvm workerJvm) {
            Process process = workerJvm.getProcess();
            int exitCode;
            try {
                exitCode = process.exitValue();
            } catch (IllegalThreadStateException ignore) {
                // process is still running
                return;
            }

            if (exitCode == 0) {
                workerJvm.setFinished();
                sendFailureOperation("Worker terminated normally", WORKER_FINISHED, workerJvm);
                return;
            }

            agent.terminateWorkerJvm(workerJvm);
            workerJVMs.remove(workerJvm.getAddress());

            sendFailureOperation(format("Worker terminated with exit code %d instead of 0", exitCode), WORKER_EXIT, workerJvm);
        }

        private void sendFailureOperation(String message, FailureType type, WorkerJvm jvm) {
            sendFailureOperation(message, type, jvm, null, null);
        }

        private void sendFailureOperation(String message, FailureType type, WorkerJvm jvm, String testId, String cause) {
            FailureOperation operation = new FailureOperation(message, type, jvm.getAddress(), agent.getPublicAddress(),
                    jvm.getHazelcastAddress(), jvm.getId(), testId, agent.getTestSuite(), cause);
            LOGGER.error(format("Detected failure on worker %s: %s", jvm.getId(), operation.getLogMessage(++failureCount)));

            try {
                Response response = agent.getAgentConnector().write(SimulatorAddress.COORDINATOR, operation);
                if (response.getFirstErrorResponseType() != ResponseType.SUCCESS) {
                    LOGGER.fatal(format("Could not send failure to coordinator! %s", operation));
                } else {
                    LOGGER.info("Failure successfully sent to Coordinator!");
                }
            } catch (SimulatorProtocolException e) {
                if (!isInterrupted()) {
                    LOGGER.fatal(format("Could not send failure to coordinator! %s", operation), e);
                }
            }
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
