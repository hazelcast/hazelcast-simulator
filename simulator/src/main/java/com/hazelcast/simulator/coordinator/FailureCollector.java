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

import com.hazelcast.simulator.common.FailureType;
import com.hazelcast.simulator.protocol.operation.FailureOperation;
import com.hazelcast.simulator.utils.CommandLineExitException;
import org.apache.log4j.Logger;

import java.io.File;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

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

    public FailureCollector(File outputDirectory) {
        this.file = new File(outputDirectory, "failures.txt");
    }

    public void addListener(FailureListener listener) {
        addListener(false, listener);
    }

    public void addListener(boolean includingPoisonPill, FailureListener listener) {
        listenerMap.put(listener, includingPoisonPill);
    }

    public void notify(FailureOperation failure) {
        boolean isFinishedFailure = false;

        FailureType failureType = failure.getType();
        if (failureType.isWorkerFinishedFailure()) {
            isFinishedFailure = true;
        }

        if (failureType.isPoisonPill()) {
            for (Map.Entry<FailureListener, Boolean> entry : listenerMap.entrySet()) {
                if (entry.getValue()) {
                    entry.getKey().onFailure(failure, isFinishedFailure, false);
                }
            }
            return;
        }

        int failureCount = criticalFailureCounter.incrementAndGet();
        String testId = failure.getTestId();
        if (testId != null) {
            hasCriticalFailuresMap.put(testId, true);
        }

        logFailure(failure, failureCount, true);

        appendText(failure.getFileMessage(), file);

        for (FailureListener failureListener : listenerMap.keySet()) {
            failureListener.onFailure(failure, isFinishedFailure, true);
        }
    }

    private void logFailure(FailureOperation failure, long failureCount, boolean isCriticalFailure) {
        int failureNumber = failureNumberGenerator.incrementAndGet();
        if (failureCount < MAX_CONSOLE_FAILURE_COUNT) {
            if (isCriticalFailure) {
                LOGGER.error(failure.getLogMessage(failureNumber));
            } else {
                LOGGER.info(failure.getLogMessage(failureNumber));
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
