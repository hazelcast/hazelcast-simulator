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
import com.hazelcast.simulator.protocol.connector.AgentConnector;
import com.hazelcast.simulator.protocol.core.Response;
import com.hazelcast.simulator.protocol.core.ResponseType;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.core.SimulatorProtocolException;
import com.hazelcast.simulator.protocol.operation.FailureOperation;
import com.hazelcast.simulator.test.FailureType;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FilenameFilter;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.simulator.test.FailureType.WORKER_EXCEPTION;
import static com.hazelcast.simulator.test.FailureType.WORKER_EXIT;
import static com.hazelcast.simulator.test.FailureType.WORKER_FINISHED;
import static com.hazelcast.simulator.test.FailureType.WORKER_OOM;
import static com.hazelcast.simulator.test.FailureType.WORKER_TIMEOUT;
import static com.hazelcast.simulator.utils.CommonUtils.sleepMillis;
import static com.hazelcast.simulator.utils.FileUtils.deleteQuiet;
import static com.hazelcast.simulator.utils.FileUtils.fileAsText;
import static com.hazelcast.simulator.utils.FormatUtils.NEW_LINE;
import static java.lang.String.format;

public class WorkerJvmFailureMonitor {

    private static final int LAST_SEEN_TIMEOUT_SECONDS = 30;
    private static final int DEFAULT_CHECK_INTERVAL_MILLIS = (int) TimeUnit.SECONDS.toMillis(1);

    private static final Logger LOGGER = Logger.getLogger(WorkerJvmFailureMonitor.class);

    private final MonitorThread monitorThread;

    private int failureCount;

    public WorkerJvmFailureMonitor(Agent agent, WorkerJvmManager workerJvmManager) {
        this(agent, workerJvmManager, DEFAULT_CHECK_INTERVAL_MILLIS);
    }

    public WorkerJvmFailureMonitor(Agent agent, WorkerJvmManager workerJvmManager, int checkIntervalMillis) {
        monitorThread = new MonitorThread(agent, workerJvmManager, checkIntervalMillis);
        monitorThread.start();
    }

    public void shutdown() {
        monitorThread.running = false;
        monitorThread.interrupt();
    }

    public void startTimeoutDetection() {
        LOGGER.info("Starting timeout detection for Workers...");
        monitorThread.updateLastSeen();
        monitorThread.detectTimeouts = true;
    }

    public void stopTimeoutDetection() {
        LOGGER.info("Stopping timeout detection for Workers...");
        monitorThread.detectTimeouts = false;
    }

    private class MonitorThread extends Thread {

        private final Agent agent;
        private final WorkerJvmManager workerJvmManager;
        private final int checkIntervalMillis;

        private volatile boolean running = true;
        private volatile boolean detectTimeouts;

        public MonitorThread(Agent agent, WorkerJvmManager workerJvmManager, int checkIntervalMillis) {
            super("WorkerJvmFailureMonitorThread");
            setDaemon(true);

            this.agent = agent;
            this.workerJvmManager = workerJvmManager;
            this.checkIntervalMillis = checkIntervalMillis;
        }

        public void run() {
            while (running) {
                try {
                    for (WorkerJvm workerJvm : workerJvmManager.getWorkerJVMs()) {
                        detectFailures(workerJvm);
                    }
                } catch (Exception e) {
                    LOGGER.fatal("Failed to scan for failures", e);
                }
                sleepMillis(checkIntervalMillis);
            }
        }

        private void updateLastSeen() {
            for (WorkerJvm workerJvm : workerJvmManager.getWorkerJVMs()) {
                workerJvm.updateLastSeen();
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

            File[] exceptionFiles = ExceptionExtensionFilter.listFiles(workerHome);
            for (File exceptionFile : exceptionFiles) {
                String content = fileAsText(exceptionFile);

                int indexOf = content.indexOf(NEW_LINE);
                String testId = content.substring(0, indexOf);
                String cause = content.substring(indexOf + 1);

                if (testId.isEmpty() || "null".equals(testId)) {
                    testId = null;
                }

                // we delete the exception file so that we don't detect the same exception again
                deleteQuiet(exceptionFile);

                sendFailureOperation("Worked ran into an unhandled exception", WORKER_EXCEPTION, workerJvm, testId, cause);
            }
        }

        private void detectOomeFailure(WorkerJvm workerJvm) {
            if (!isOomeFound(workerJvm.getWorkerHome())) {
                return;
            }
            workerJvm.setOomeDetected();

            sendFailureOperation("Worker ran into an OOME", WORKER_OOM, workerJvm);
        }

        private boolean isOomeFound(File workerHome) {
            File oomeFile = new File(workerHome, "worker.oome");
            if (oomeFile.exists()) {
                return true;
            }

            // if we find the hprof file, we also know there is an OOME. The problem with the worker.oome file is that it is
            // created after the heap dump is done, and creating the heap dump can take a lot of time. And then the system could
            // think there is another problem (e.g. lack of inactivity; or timeouts). This hides the OOME.
            File[] hprofFiles = HProfExtensionFilter.listFiles(workerHome);
            return (hprofFiles.length > 0);
        }

        private void detectInactivity(WorkerJvm workerJvm) {
            if (!detectTimeouts) {
                return;
            }

            long elapsed = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - workerJvm.getLastSeen());
            if (elapsed > LAST_SEEN_TIMEOUT_SECONDS) {
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

            workerJvmManager.shutdown(workerJvm);

            sendFailureOperation(format("Worker terminated with exit code %d instead of 0", exitCode), WORKER_EXIT, workerJvm);
        }

        private void sendFailureOperation(String message, FailureType type, WorkerJvm jvm) {
            sendFailureOperation(message, type, jvm, null, null);
        }

        private void sendFailureOperation(String message, FailureType type, WorkerJvm jvm, String testId, String cause) {
            boolean isFailure = (type != WORKER_FINISHED);
            SimulatorAddress workerAddress = jvm.getAddress();
            FailureOperation operation = new FailureOperation(message, type, workerAddress, agent.getPublicAddress(),
                    jvm.getHazelcastAddress(), jvm.getId(), testId, agent.getTestSuite(), cause);
            if (isFailure) {
                LOGGER.error(format("Detected failure on Worker %s: %s", jvm.getId(), operation.getLogMessage(++failureCount)));
            }

            AgentConnector agentConnector = agent.getAgentConnector();
            if (type.isWorkerFinishedFailure()) {
                String finishedType = (isFailure) ? "failed" : "finished";
                LOGGER.info(format("Removing %s Worker %s from configuration...", finishedType, workerAddress));
                agentConnector.removeWorker(workerAddress.getWorkerIndex());
            }

            try {
                Response response = agentConnector.write(SimulatorAddress.COORDINATOR, operation);
                if (response.getFirstErrorResponseType() != ResponseType.SUCCESS) {
                    LOGGER.error(format("Could not send failure to coordinator! %s", operation));
                } else {
                    LOGGER.info("Failure successfully sent to Coordinator!");
                }
            } catch (SimulatorProtocolException e) {
                if (!isInterrupted()) {
                    LOGGER.error(format("Could not send failure to coordinator! %s", operation), e);
                }
            }
        }
    }

    static class ExceptionExtensionFilter implements FilenameFilter {

        private static final ExceptionExtensionFilter INSTANCE = new ExceptionExtensionFilter();
        private static final File[] EMPTY_FILES = new File[0];

        static File[] listFiles(File workerHome) {
            File[] exceptionFiles = workerHome.listFiles(ExceptionExtensionFilter.INSTANCE);
            if (exceptionFiles == null) {
                return EMPTY_FILES;
            }
            return exceptionFiles;
        }

        @Override
        public boolean accept(File dir, String name) {
            return name.endsWith(".exception");
        }
    }

    static class HProfExtensionFilter implements FilenameFilter {

        private static final HProfExtensionFilter INSTANCE = new HProfExtensionFilter();
        private static final File[] EMPTY_FILES = new File[0];

        static File[] listFiles(File workerHome) {
            File[] hprofFiles = workerHome.listFiles(HProfExtensionFilter.INSTANCE);
            if (hprofFiles == null) {
                return EMPTY_FILES;
            }
            return hprofFiles;
        }

        @Override
        public boolean accept(File dir, String name) {
            return name.endsWith(".hprof");
        }
    }
}
