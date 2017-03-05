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

import com.hazelcast.simulator.coordinator.operations.FailureOperation;
import com.hazelcast.simulator.coordinator.registry.ComponentRegistry;
import com.hazelcast.simulator.coordinator.registry.TestData;
import com.hazelcast.simulator.coordinator.registry.WorkerData;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import org.apache.log4j.Logger;

import java.io.File;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.simulator.common.FailureType.WORKER_CREATE_ERROR;
import static com.hazelcast.simulator.common.FailureType.WORKER_NORMAL_EXIT;
import static com.hazelcast.simulator.utils.FileUtils.appendText;
import static com.hazelcast.simulator.utils.FormatUtils.HORIZONTAL_RULER;
import static java.lang.String.format;

/**
 * Responsible for storing and formatting failures from Simulator workers.
 */
public class FailureCollector {

    private static final int MAX_CONSOLE_FAILURE_COUNT = 25;

    private static final Logger LOGGER = Logger.getLogger(FailureCollector.class);

    private final AtomicInteger failureNumberGenerator = new AtomicInteger();
    private final ConcurrentMap<FailureListener, Boolean> listenerMap = new ConcurrentHashMap<FailureListener, Boolean>();

    private final AtomicInteger nonCriticalFailureCounter = new AtomicInteger();
    private final AtomicInteger criticalFailureCounter = new AtomicInteger();
    private final ConcurrentMap<String, Boolean> hasCriticalFailuresMap = new ConcurrentHashMap<String, Boolean>();

    private final File file;
    private final ComponentRegistry componentRegistry;

    public FailureCollector(File outputDirectory, ComponentRegistry componentRegistry) {
        this.file = new File(outputDirectory, "failures.txt");
        this.componentRegistry = componentRegistry;
    }

    public void addListener(FailureListener listener) {
        listenerMap.put(listener, false);
    }

    public void notify(FailureOperation failure) {
        failure = enrich(failure);

        SimulatorAddress workerAddress = failure.getWorkerAddress();
        if (workerAddress != null && failure.getType() != WORKER_CREATE_ERROR) {
            WorkerData worker = componentRegistry.findWorker(workerAddress);
            if (worker == null) {
                // we are not interested in failures of workers that aren't registered any longer.
                return;
            }

            // if the failure is the terminal for that workers, we need to remove it from the component registry
            if (failure.getType().isTerminal()) {
                LOGGER.info("Removing worker " + worker.getAddress()
                        + " from componentRegistry due to [" + failure.getType() + "]");
                componentRegistry.removeWorker(worker.getAddress());
            }

            // if we don't care for the failure, we are done; no need to log anything.
            if (worker.isIgnoreFailures() || failure.getType() == WORKER_NORMAL_EXIT) {
                return;
            }
        }

        int failureCount = criticalFailureCounter.incrementAndGet();
        String testId = failure.getTestId();
        if (testId != null) {
            hasCriticalFailuresMap.put(testId, true);
        }

        logFailure(failure, failureCount);

        appendText(failure.getFileMessage(), file);

        for (FailureListener failureListener : listenerMap.keySet()) {
            failureListener.onFailure(failure, failure.getType().isTerminal(), true);
        }
    }

    private FailureOperation enrich(FailureOperation failure) {
        String testId = failure.getTestId();
        if (testId != null) {
            TestData testData = componentRegistry.getTest(testId);
            if (testData != null) {
                failure.setTestCase(testData.getTestCase());
                failure.setDuration(System.currentTimeMillis() - testData.getStartTimeMillis());
            }
        }
        return failure;
    }

    private void logFailure(FailureOperation failure, long failureCount) {
        if (failure.getType() == WORKER_CREATE_ERROR) {
            // create error we don't need to log; they will be logged by the CreateWorkersTask
            return;
        }

        int failureNumber = failureNumberGenerator.incrementAndGet();
        if (failureCount < MAX_CONSOLE_FAILURE_COUNT) {
            if (failure.getType().isTerminal()) {
                LOGGER.warn(failure.getLogMessage(failureNumber));
            } else {
                LOGGER.info(failure.getLogMessage(failureNumber));
            }
        } else if (failureNumber == MAX_CONSOLE_FAILURE_COUNT) {
            if (failure.getType().isTerminal()) {
                LOGGER.warn(format("Maximum number of critical failures has been reached. "
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

    public boolean hasCriticalFailure() {
        return criticalFailureCounter.get() > 0;
    }

    boolean hasCriticalFailure(String testId) {
        return hasCriticalFailuresMap.containsKey(testId);
    }

    void logFailureInfo() {
        int criticalFailureCount = criticalFailureCounter.get();
        int nonCriticalFailureCount = nonCriticalFailureCounter.get();
        if (criticalFailureCount > 0 || nonCriticalFailureCount > 0) {
            if (criticalFailureCount > 0) {
                LOGGER.warn(HORIZONTAL_RULER);
                LOGGER.warn(criticalFailureCount + " critical failures have been detected!!!");
                LOGGER.warn(HORIZONTAL_RULER);
            } else {
                LOGGER.warn(HORIZONTAL_RULER);
                LOGGER.warn(nonCriticalFailureCount + " non-critical failures have been detected!");
                LOGGER.warn(HORIZONTAL_RULER);
            }
            return;
        }
        LOGGER.info(HORIZONTAL_RULER);
        LOGGER.info("No failures have been detected!");
        LOGGER.info(HORIZONTAL_RULER);
    }
}
