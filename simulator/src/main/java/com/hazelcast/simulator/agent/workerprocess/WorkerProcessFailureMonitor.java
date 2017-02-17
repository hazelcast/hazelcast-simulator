/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.simulator.agent.workerprocess;

import com.hazelcast.simulator.common.FailureType;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FilenameFilter;

import static com.hazelcast.simulator.common.FailureType.WORKER_ABNORMAL_EXIT;
import static com.hazelcast.simulator.common.FailureType.WORKER_EXCEPTION;
import static com.hazelcast.simulator.common.FailureType.WORKER_NORMAL_EXIT;
import static com.hazelcast.simulator.common.FailureType.WORKER_OOME;
import static com.hazelcast.simulator.common.FailureType.WORKER_TIMEOUT;
import static com.hazelcast.simulator.utils.CommonUtils.sleepMillis;
import static com.hazelcast.simulator.utils.FileUtils.deleteQuiet;
import static com.hazelcast.simulator.utils.FileUtils.fileAsText;
import static com.hazelcast.simulator.utils.FileUtils.rename;
import static com.hazelcast.simulator.utils.FormatUtils.NEW_LINE;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class WorkerProcessFailureMonitor {

    private static final int DEFAULT_CHECK_INTERVAL_MILLIS = (int) SECONDS.toMillis(1);

    private static final Logger LOGGER = Logger.getLogger(WorkerProcessFailureMonitor.class);

    private final MonitorThread monitorThread;

    public WorkerProcessFailureMonitor(WorkerProcessFailureHandler failureHandler,
                                       WorkerProcessManager workerProcessManager,
                                       int lastSeenTimeoutSeconds) {
        this(failureHandler, workerProcessManager, lastSeenTimeoutSeconds, DEFAULT_CHECK_INTERVAL_MILLIS);
    }

    WorkerProcessFailureMonitor(WorkerProcessFailureHandler failureHandler,
                                WorkerProcessManager workerProcessManager,
                                int lastSeenTimeoutSeconds,
                                int checkIntervalMillis) {
        monitorThread = new MonitorThread(failureHandler, workerProcessManager, lastSeenTimeoutSeconds, checkIntervalMillis);
    }

    public void start() {
        monitorThread.start();
    }

    public void shutdown() {
        monitorThread.running = false;
        monitorThread.interrupt();
    }

    public void startTimeoutDetection() {
        if (monitorThread.lastSeenTimeoutSeconds > 0) {
            LOGGER.info("Starting timeout detection for Workers...");
            monitorThread.updateLastSeen();
            monitorThread.detectTimeouts = true;
        }
    }

    public void stopTimeoutDetection() {
        if (monitorThread.lastSeenTimeoutSeconds > 0) {
            LOGGER.info("Stopping timeout detection for Workers...");
            monitorThread.detectTimeouts = false;
        }
    }

    private final class MonitorThread extends Thread {

        private final WorkerProcessFailureHandler failureHandler;
        private final WorkerProcessManager workerProcessManager;
        private final int lastSeenTimeoutSeconds;
        private final int checkIntervalMillis;

        private volatile boolean running = true;
        private volatile boolean detectTimeouts;

        private MonitorThread(WorkerProcessFailureHandler failureHandler,
                              WorkerProcessManager workerProcessManager,
                              int lastSeenTimeoutSeconds,
                              int checkIntervalMillis) {
            super("WorkerJvmFailureMonitorThread");
            setDaemon(true);

            this.failureHandler = failureHandler;
            this.workerProcessManager = workerProcessManager;
            this.lastSeenTimeoutSeconds = lastSeenTimeoutSeconds;
            this.checkIntervalMillis = checkIntervalMillis;
        }

        @Override
        public void run() {
            while (running) {
                try {
                    for (WorkerProcess workerProcess : workerProcessManager.getWorkerProcesses()) {

                        detectFailures(workerProcess);

                        if (workerProcess.isFinished()) {
                            workerProcessManager.remove(workerProcess);
                        }
                    }
                } catch (Exception e) {
                    LOGGER.fatal("Failed to scan for failures", e);
                }
                sleepMillis(checkIntervalMillis);
            }
        }

        private void updateLastSeen() {
            for (WorkerProcess workerProcess : workerProcessManager.getWorkerProcesses()) {
                workerProcess.updateLastSeen();
            }
        }

        private void detectFailures(WorkerProcess workerProcess) {
            detectExceptions(workerProcess);

            if (workerProcess.isOomeDetected()) {
                return;
            }

            detectOomeFailure(workerProcess);
            detectInactivity(workerProcess);
            detectUnexpectedExit(workerProcess);
        }

        private void detectExceptions(WorkerProcess workerProcess) {
            File workerHome = workerProcess.getWorkerHome();
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

                // we delete or rename the exception file so that we don't detect the same exception again
                boolean send = failureHandler.handle("Worked ran into an unhandled exception", WORKER_EXCEPTION, workerProcess,
                        testId, cause);

                if (send) {
                    deleteQuiet(exceptionFile);
                } else {
                    rename(exceptionFile, new File(exceptionFile.getParentFile(), exceptionFile.getName() + ".sendFailure"));
                }
            }
        }

        private void detectOomeFailure(WorkerProcess workerProcess) {
            if (!isOomeFound(workerProcess.getWorkerHome())) {
                return;
            }
            workerProcess.setOomeDetected();

            sendFailureOperation("Worker ran into an OOME", WORKER_OOME, workerProcess);
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

        private void detectInactivity(WorkerProcess workerProcess) {
            if (!detectTimeouts) {
                return;
            }

            long elapsed = MILLISECONDS.toSeconds(System.currentTimeMillis() - workerProcess.getLastSeen());
            if (elapsed > 0 && elapsed % lastSeenTimeoutSeconds == 0) {
                sendFailureOperation(format("Worker has not sent a message for %d seconds", elapsed), WORKER_TIMEOUT,
                        workerProcess);
            }
        }

        private void detectUnexpectedExit(WorkerProcess workerProcess) {
            Process process = workerProcess.getProcess();
            int exitCode;
            try {
                exitCode = process.exitValue();
            } catch (IllegalThreadStateException ignore) {
                // process is still running
                return;
            }

            if (exitCode == 0) {
                workerProcess.setFinished();
                sendFailureOperation("Worker terminated normally", WORKER_NORMAL_EXIT, workerProcess);
                return;
            }

            workerProcessManager.shutdown(workerProcess);

            sendFailureOperation(format("Worker terminated with exit code %d instead of 0", exitCode), WORKER_ABNORMAL_EXIT,
                    workerProcess);
        }

        private void sendFailureOperation(String message, FailureType type, WorkerProcess workerProcess) {
            failureHandler.handle(message, type, workerProcess, null, null);
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
