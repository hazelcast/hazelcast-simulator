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
package com.hazelcast.simulator.coordinator;

import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.operation.FailureOperation;
import com.hazelcast.simulator.protocol.registry.ComponentRegistry;
import com.hazelcast.simulator.test.FailureType;
import com.hazelcast.simulator.utils.CommandLineExitException;
import org.apache.log4j.Logger;

import java.io.File;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.simulator.utils.CommonUtils.getElapsedSeconds;
import static com.hazelcast.simulator.utils.CommonUtils.sleepMillis;
import static com.hazelcast.simulator.utils.FileUtils.appendText;
import static com.hazelcast.simulator.utils.FormatUtils.HORIZONTAL_RULER;
import static java.lang.String.format;

/**
 * Responsible for storing and formatting failures from Simulator workers.
 */
public class FailureContainer {

    private static final int FINISHED_WORKERS_SLEEP_MILLIS = 500;
    private static final int MAX_CONSOLE_FAILURE_COUNT = 25;

    private static final Logger LOGGER = Logger.getLogger(FailureContainer.class);

    private final AtomicInteger failureNumberGenerator = new AtomicInteger();
    private final ConcurrentMap<SimulatorAddress, FailureType> finishedWorkers
            = new ConcurrentHashMap<SimulatorAddress, FailureType>();
    private final ConcurrentMap<FailureListener, Boolean> listenerMap = new ConcurrentHashMap<FailureListener, Boolean>();

    private final AtomicInteger nonCriticalFailureCounter = new AtomicInteger();
    private final AtomicInteger criticalFailureCounter = new AtomicInteger();
    private final ConcurrentMap<String, Boolean> hasCriticalFailuresMap = new ConcurrentHashMap<String, Boolean>();

    private final File file;
    private final ComponentRegistry componentRegistry;
    private final Set<FailureType> nonCriticalFailures;

    public FailureContainer(File outputDirectory,
                            ComponentRegistry componentRegistry,
                            Set<FailureType> nonCriticalFailures) {
        this.file = new File(outputDirectory, "failures.txt");
        this.componentRegistry = componentRegistry;
        this.nonCriticalFailures = nonCriticalFailures;
    }

    public void addListener(FailureListener listener) {
        listenerMap.put(listener, true);
    }

    public void addFailureOperation(FailureOperation operation) {
        boolean isFinishedFailure = false;
        boolean isCriticalFailure;

        FailureType failureType = operation.getType();
        if (failureType.isWorkerFinishedFailure()) {
            SimulatorAddress workerAddress = operation.getWorkerAddress();
            finishedWorkers.put(workerAddress, failureType);
            componentRegistry.removeWorker(workerAddress);
            isFinishedFailure = true;
        }

        if (failureType.isPoisonPill()) {
            return;
        }

        int failureCount;
        if (nonCriticalFailures.contains(failureType)) {
            isCriticalFailure = false;
            failureCount = nonCriticalFailureCounter.incrementAndGet();
        } else {
            failureCount = criticalFailureCounter.incrementAndGet();
            String testId = operation.getTestId();
            if (testId != null) {
                hasCriticalFailuresMap.put(testId, true);
            }
            isCriticalFailure = true;
        }

        logFailure(operation, failureCount, isCriticalFailure);

        appendText(operation.getFileMessage(), file);

        for (FailureListener failureListener : listenerMap.keySet()) {
            failureListener.onFailure(operation, isFinishedFailure, isCriticalFailure);
        }
    }

    private void logFailure(FailureOperation operation, long failureCount, boolean isCriticalFailure) {
        int failureNumber = failureNumberGenerator.incrementAndGet();
        if (failureCount < MAX_CONSOLE_FAILURE_COUNT) {
            if (isCriticalFailure) {
                LOGGER.error(operation.getLogMessage(failureNumber));
            } else {
                LOGGER.info(operation.getLogMessage(failureNumber));
            }
        } else if (failureNumber == MAX_CONSOLE_FAILURE_COUNT) {
            if (isCriticalFailure) {
                LOGGER.error(format("Maximum number of critical failures has been reached. "
                        + "Additional failures can be found in '%s'", file.getAbsolutePath()));
            } else {
                LOGGER.info(format("Maximum number of non critical failures has been reached. "
                        + "Additional failures can be found in '%s'", file.getAbsolutePath()));
            }
        }
    }

    int getFailureCount() {
        return criticalFailureCounter.get() + nonCriticalFailureCounter.get();
    }

    boolean hasCriticalFailure() {
        return criticalFailureCounter.get() > 0;
    }

    boolean hasCriticalFailure(String testId) {
        return hasCriticalFailuresMap.containsKey(testId);
    }

    Set<SimulatorAddress> getFinishedWorkers() {
        return finishedWorkers.keySet();
    }

    boolean waitForWorkerShutdown(int expectedWorkerCount, int timeoutSeconds) {
        long started = System.nanoTime();
        LOGGER.info(format("Waiting up to %d seconds for shutdown of %d Workers...", timeoutSeconds, expectedWorkerCount));
        long timeoutTimestamp = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(timeoutSeconds);
        while (finishedWorkers.size() < expectedWorkerCount && System.currentTimeMillis() < timeoutTimestamp) {
            sleepMillis(FINISHED_WORKERS_SLEEP_MILLIS);
        }
        int remainingWorkers = expectedWorkerCount - finishedWorkers.size();
        if (remainingWorkers > 0) {
            LOGGER.warn(format("Aborted waiting for shutdown of all Workers (%d still running)...", remainingWorkers));
            return false;
        }
        LOGGER.info(format("Finished shutdown of all Workers (%d seconds)", getElapsedSeconds(started)));
        return true;
    }

    void logFailureInfo() {
        int criticalFailureCount = criticalFailureCounter.get();
        int nonCriticalFailureCount = nonCriticalFailureCounter.get();
        if (criticalFailureCount > 0 || nonCriticalFailureCount > 0) {
            if (criticalFailureCount > 0) {
                LOGGER.fatal(HORIZONTAL_RULER);
                LOGGER.fatal(criticalFailureCount + " critical failures have been detected!!!");
                LOGGER.fatal(HORIZONTAL_RULER);
                throw new CommandLineExitException(criticalFailureCount + " critical failures have been detected");
            } else {
                LOGGER.fatal(HORIZONTAL_RULER);
                LOGGER.fatal(nonCriticalFailureCount + " non-critical failures have been detected!");
                LOGGER.fatal(HORIZONTAL_RULER);
            }
            return;
        }
        LOGGER.info(HORIZONTAL_RULER);
        LOGGER.info("No failures have been detected!");
        LOGGER.info(HORIZONTAL_RULER);
    }
}
