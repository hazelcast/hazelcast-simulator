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
package com.hazelcast.simulator.coordinator;

import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.utils.CommandLineExitException;
import org.apache.log4j.Logger;

import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.simulator.utils.CommonUtils.sleepMillis;
import static com.hazelcast.simulator.utils.FormatUtils.HORIZONTAL_RULER;
import static java.lang.String.format;

final class CoordinatorUtils {

    public static final int FINISHED_WORKER_TIMEOUT_SECONDS = 120;
    public static final int FINISHED_WORKERS_SLEEP_MILLIS = 500;

    private static final Logger LOGGER = Logger.getLogger(CoordinatorUtils.class);

    private CoordinatorUtils() {
    }

    static boolean waitForWorkerShutdown(int expectedFinishedWorkerCount, Set<SimulatorAddress> finishedWorkers,
                                         int timeoutSeconds) {
        LOGGER.info(format("Waiting %d seconds for shutdown of %d workers...", timeoutSeconds, expectedFinishedWorkerCount));
        long timeoutTimestamp = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(timeoutSeconds);
        while (finishedWorkers.size() < expectedFinishedWorkerCount && System.currentTimeMillis() < timeoutTimestamp) {
            sleepMillis(FINISHED_WORKERS_SLEEP_MILLIS);
        }
        int remainingWorkers = expectedFinishedWorkerCount - finishedWorkers.size();
        if (remainingWorkers > 0) {
            LOGGER.warn(format("Aborted waiting for shutdown of all workers (%d still running)...", remainingWorkers));
            return false;
        }
        LOGGER.info("Shutdown of all workers completed...");
        return true;
    }

    static void logFailureInfo(int failureCount) {
        if (failureCount > 0) {
            LOGGER.fatal(HORIZONTAL_RULER);
            LOGGER.fatal(failureCount + " failures have been detected!!!");
            LOGGER.fatal(HORIZONTAL_RULER);
            throw new CommandLineExitException(failureCount + " failures have been detected");
        }
        LOGGER.info(HORIZONTAL_RULER);
        LOGGER.info("No failures have been detected!");
        LOGGER.info(HORIZONTAL_RULER);
    }
}
