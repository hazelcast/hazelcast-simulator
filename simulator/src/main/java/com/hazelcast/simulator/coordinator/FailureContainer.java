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
import com.hazelcast.simulator.test.TestSuite;
import com.hazelcast.simulator.utils.CommandLineExitException;
import org.apache.log4j.Logger;

import java.io.File;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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

    private static final Logger LOGGER = Logger.getLogger(FailureContainer.class);

    private final AtomicInteger failureCount = new AtomicInteger();
    private final ConcurrentMap<SimulatorAddress, FailureType> finishedWorkers
            = new ConcurrentHashMap<SimulatorAddress, FailureType>();
    private final ConcurrentHashMap<FailureListener, Boolean> listenerMap = new ConcurrentHashMap<FailureListener, Boolean>();

    private final AtomicBoolean hasCriticalFailure = new AtomicBoolean();
    private final ConcurrentMap<String, Boolean> hasCriticalFailuresMap = new ConcurrentHashMap<String, Boolean>();

    private final File file;
    private final ComponentRegistry componentRegistry;
    private final Set<FailureType> nonCriticalFailures;

    public FailureContainer(TestSuite testSuite, ComponentRegistry componentRegistry) {
        this(testSuite.getId(), componentRegistry, testSuite.getTolerableFailures());
    }

    public FailureContainer(String testSuiteId, ComponentRegistry componentRegistry) {
        this(testSuiteId, componentRegistry, Collections.<FailureType>emptySet());
    }

    public FailureContainer(String testSuiteId, ComponentRegistry componentRegistry, Set<FailureType> nonCriticalFailures) {
        this.file = new File("failures-" + testSuiteId + ".txt");
        this.componentRegistry = componentRegistry;
        this.nonCriticalFailures = nonCriticalFailures;
    }

    public void addListener(FailureListener listener) {
        listenerMap.put(listener, true);
    }

    public void addFailureOperation(FailureOperation operation) {
        FailureType failureType = operation.getType();
        if (failureType.isWorkerFinishedFailure()) {
            SimulatorAddress workerAddress = operation.getWorkerAddress();
            finishedWorkers.put(workerAddress, failureType);
            componentRegistry.removeWorker(workerAddress);
        }
        if (failureType.isPoisonPill()) {
            return;
        }

        int failureNumber = failureCount.incrementAndGet();

        if (!nonCriticalFailures.contains(failureType)) {
            hasCriticalFailure.set(true);
            String testId = operation.getTestId();
            if (testId != null) {
                hasCriticalFailuresMap.put(testId, true);
            }
        }

        LOGGER.error(operation.getLogMessage(failureNumber));
        appendText(operation.getFileMessage(), file);

        for (FailureListener failureListener : listenerMap.keySet()) {
            failureListener.onFailure(operation);
        }
    }

    int getFailureCount() {
        return failureCount.get();
    }

    boolean hasCriticalFailure() {
        return hasCriticalFailure.get();
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
        int tmpFailureCount = failureCount.get();
        if (tmpFailureCount > 0) {
            if (hasCriticalFailure.get()) {
                LOGGER.fatal(HORIZONTAL_RULER);
                LOGGER.fatal(tmpFailureCount + " failures have been detected!!!");
                LOGGER.fatal(HORIZONTAL_RULER);
                throw new CommandLineExitException(tmpFailureCount + " failures have been detected");
            } else {
                LOGGER.fatal(HORIZONTAL_RULER);
                LOGGER.fatal(tmpFailureCount + " non-critical failures have been detected!");
                LOGGER.fatal(HORIZONTAL_RULER);
            }
            return;
        }
        LOGGER.info(HORIZONTAL_RULER);
        LOGGER.info("No failures have been detected!");
        LOGGER.info(HORIZONTAL_RULER);
    }
}
