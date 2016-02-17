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
package com.hazelcast.simulator.worker;

import com.hazelcast.simulator.common.ShutdownThread;
import com.hazelcast.simulator.protocol.connector.WorkerConnector;
import com.hazelcast.simulator.utils.NativeUtils;
import org.apache.log4j.Logger;

import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.simulator.utils.FileUtils.writeText;

public final class IntegrationTestWorker implements Worker {

    private static final int DEFAULT_TIMEOUT_SECONDS = 5;

    private static final Logger LOGGER = Logger.getLogger(IntegrationTestWorker.class);

    private final AtomicBoolean shutdownStarted = new AtomicBoolean();
    private final CountDownLatch shutdownComplete = new CountDownLatch(1);

    IntegrationTestWorker() {
        LOGGER.info("Starting IntegrationTestWorker...");

        Runtime.getRuntime().addShutdownHook(new IntegrationTestWorkerShutdownThread(shutdownStarted, true, shutdownComplete));

        int pid = NativeUtils.getPID();
        LOGGER.info("PID: " + pid);
        File pidFile = new File("worker.pid");
        writeText("" + pid, pidFile);

        File addressFile = new File("worker.address");
        writeText("127.0.0.1:5701", addressFile);
    }

    void awaitShutdown(int timeoutSeconds) throws Exception {
        LOGGER.info("Waiting for shutdown...");
        boolean success = shutdownComplete.await(timeoutSeconds, TimeUnit.SECONDS);

        if (success) {
            LOGGER.info("Done!");
        } else {
            LOGGER.warn("IntegrationTestWorker timed out!");
        }
    }

    @Override
    public void shutdown(boolean shutdownLog4j) {
        new IntegrationTestWorkerShutdownThread(shutdownStarted, false, shutdownComplete).start();
    }

    @Override
    public boolean startPerformanceMonitor() {
        return true;
    }

    @Override
    public void shutdownPerformanceMonitor() {
    }

    @Override
    public WorkerConnector getWorkerConnector() {
        return null;
    }

    @Override
    public String getPublicIpAddress() {
        return null;
    }

    public static void main(String[] args) throws Exception {
        IntegrationTestWorker worker = new IntegrationTestWorker();
        worker.awaitShutdown(DEFAULT_TIMEOUT_SECONDS);
    }

    private static final class IntegrationTestWorkerShutdownThread extends ShutdownThread {

        private IntegrationTestWorkerShutdownThread(AtomicBoolean shutdownStarted, boolean shutdownLog4j,
                                                    CountDownLatch shutdownComplete) {
            super("IntegrationTestWorkerShutdownThread", shutdownStarted, shutdownLog4j, shutdownComplete);
        }

        @Override
        public void doRun() {
            LOGGER.info("Stopping IntegrationTestWorker...");
        }
    }
}
