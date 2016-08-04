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
package com.hazelcast.simulator.worker.performance;

import com.hazelcast.simulator.probes.Probe;
import com.hazelcast.simulator.probes.impl.HdrProbe;
import com.hazelcast.simulator.protocol.connector.ServerConnector;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.operation.PerformanceStatsOperation;
import com.hazelcast.simulator.test.TestContainer;
import org.HdrHistogram.Histogram;
import org.apache.log4j.Logger;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.simulator.utils.CommonUtils.joinThread;
import static com.hazelcast.simulator.utils.CommonUtils.sleepNanos;
import static com.hazelcast.simulator.worker.performance.PerformanceStats.INTERVAL_LATENCY_PERCENTILE;
import static java.util.concurrent.TimeUnit.*;

/**
 * Monitors the performance of all running Simulator Tests on {@link com.hazelcast.simulator.worker.MemberWorker}
 * and {@link com.hazelcast.simulator.worker.ClientWorker} instances.
 */
public class WorkerPerformanceMonitor {

    private static final int SHUTDOWN_TIMEOUT_SECONDS = 10;
    private static final long WAIT_FOR_TEST_CONTAINERS_DELAY_NANOS = MILLISECONDS.toNanos(100);
    private static final Logger LOGGER = Logger.getLogger(WorkerPerformanceMonitor.class);

    private final WorkerPerformanceMonitorThread thread;
    private final AtomicBoolean shutdown = new AtomicBoolean();

    public WorkerPerformanceMonitor(ServerConnector serverConnector, Collection<TestContainer> testContainers,
                                    int workerPerformanceMonitorInterval, TimeUnit workerPerformanceIntervalTimeUnit) {
        long intervalNanos = workerPerformanceIntervalTimeUnit.toNanos(workerPerformanceMonitorInterval);
        this.thread = new WorkerPerformanceMonitorThread(serverConnector, testContainers, intervalNanos);
        thread.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(Thread t, Throwable e) {
                LOGGER.fatal(e.getMessage(), e);
            }
        });
    }

    public void start() {
        thread.start();
    }

    public void shutdown() {
        if (!shutdown.compareAndSet(false, true)) {
            return;
        }

        joinThread(thread, MINUTES.toMillis(SHUTDOWN_TIMEOUT_SECONDS));
    }

    /**
     * Thread to monitor the performance of Simulator Tests.
     *
     * Iterates over all {@link TestContainer} to retrieve performance values from all {@link Probe} instances.
     * Sends performance numbers as {@link PerformanceStats} to the Coordinator.
     * Writes performance stats to files.
     *
     * Holds one {@link TestPerformanceTracker} instance per Simulator Test.
     */
    private final class WorkerPerformanceMonitorThread extends Thread {

        private final PerformanceLogWriter globalPerformanceLogWriter;
        private final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
        private final Map<String, TestPerformanceTracker> trackers = new ConcurrentHashMap<String, TestPerformanceTracker>();
        private final ServerConnector serverConnector;
        private final Collection<TestContainer> testContainers;
        private final long intervalNanos;

        private WorkerPerformanceMonitorThread(ServerConnector serverConnector,
                                               Collection<TestContainer> testContainers,
                                               long intervalNanos) {
            super("WorkerPerformanceMonitor");
            setDaemon(true);
            this.serverConnector = serverConnector;
            this.testContainers = testContainers;
            this.intervalNanos = intervalNanos;
            this.globalPerformanceLogWriter = new PerformanceLogWriter(new File("performance.csv"));
        }

        @Override
        public void run() {
            while (!shutdown.get()) {
                long startedNanos = System.nanoTime();
                long currentTimestamp = System.currentTimeMillis();

                boolean runningTestFound = refreshTests(currentTimestamp);
                updateTrackers(currentTimestamp);
                sendPerformanceStats();
                writeStatsToFiles(currentTimestamp);
                purgeDeadTrackers(currentTimestamp);

                long elapsedNanos = System.nanoTime() - startedNanos;
                if (intervalNanos > elapsedNanos) {
                    if (runningTestFound) {
                        sleepNanos(intervalNanos - elapsedNanos);
                    } else {
                        sleepNanos(WAIT_FOR_TEST_CONTAINERS_DELAY_NANOS - elapsedNanos);
                    }
                } else {
                    LOGGER.warn("WorkerPerformanceMonitorThread.run() took " + NANOSECONDS.toMillis(elapsedNanos) + " ms");
                }
            }
        }

        private boolean refreshTests(long currentTimestamp) {
            boolean runningTestFound = false;

            for (TestContainer testContainer : testContainers) {
                if (!testContainer.isRunning()) {
                    continue;
                }

                String testId = testContainer.getTestContext().getTestId();
                TestPerformanceTracker tracker = trackers.get(testId);
                if (tracker == null) {
                    tracker = new TestPerformanceTracker(testContainer);
                    trackers.put(testId, tracker);
                }

                // we set the lastSeen timestamp, so we can easily purge dead trackers
                tracker.setLastSeen(currentTimestamp);
                runningTestFound = true;
            }

            return runningTestFound;
        }

        // we remove every MonitoredTest that doesn't have the desired timestamp
        private void purgeDeadTrackers(long currentTimestamp) {
            for (TestPerformanceTracker tracker : trackers.values()) {
                // purge the testData if it wasn't seen in the current run
                if (tracker.getLastSeen() == currentTimestamp) {
                    continue;
                }

                trackers.remove(tracker.getTestId());
            }
        }

        private void updateTrackers(long currentTimestamp) {
            for (TestPerformanceTracker tracker : trackers.values()) {
                updateTrackers(currentTimestamp, tracker);
            }
        }

        private void updateTrackers(long currentTimestamp, TestPerformanceTracker tracker) {
            TestContainer testContainer = tracker.getTestContainer();
            Map<String, Probe> probeMap = testContainer.getProbeMap();
            Map<String, Histogram> intervalHistograms = new HashMap<String, Histogram>(probeMap.size());

            long intervalPercentileLatency = -1;
            double intervalMean = -1;
            long intervalMaxLatency = -1;

            long iterations = testContainer.iteration();
            long intervalOperationCount = iterations - tracker.getLastIterations();

            for (Map.Entry<String, Probe> entry : probeMap.entrySet()) {
                String probeName = entry.getKey();
                Probe probe = entry.getValue();
                if (!(probe instanceof HdrProbe)) {
                    continue;
                }

                HdrProbe hdrProbe = (HdrProbe) probe;
                Histogram intervalHistogram = hdrProbe.getIntervalHistogram();
                intervalHistograms.put(probeName, intervalHistogram);

                long percentileValue = intervalHistogram.getValueAtPercentile(INTERVAL_LATENCY_PERCENTILE);
                if (percentileValue > intervalPercentileLatency) {
                    intervalPercentileLatency = percentileValue;
                }

                double meanLatency = intervalHistogram.getMean();
                if (meanLatency > intervalMean) {
                    intervalMean = meanLatency;
                }

                long maxValue = intervalHistogram.getMaxValue();
                if (maxValue > intervalMaxLatency) {
                    intervalMaxLatency = maxValue;
                }

                if (probe.isPartOfTotalThroughput()) {
                    intervalOperationCount += intervalHistogram.getTotalCount();
                }
            }

            tracker.update(
                    intervalHistograms,
                    intervalPercentileLatency,
                    intervalMean,
                    intervalMaxLatency,
                    intervalOperationCount,
                    iterations,
                    currentTimestamp);
        }

        private void sendPerformanceStats() {
            PerformanceStatsOperation operation = new PerformanceStatsOperation();

            for (TestPerformanceTracker tracker : trackers.values()) {
                if (tracker.isUpdated()) {
                    operation.addPerformanceStats(tracker.getTestId(), tracker.createPerformanceStats());
                }
            }

            if (operation.getPerformanceStats().size() > 0) {
                serverConnector.submit(SimulatorAddress.COORDINATOR, operation);
            }
        }

        private void writeStatsToFiles(long currentTimestamp) {
            if (trackers.isEmpty()) {
                return;
            }

            String dateString = simpleDateFormat.format(new Date(currentTimestamp));
            long globalIntervalOperationCount = 0;
            long globalOperationsCount = 0;
            double globalIntervalThroughput = 0;

            for (TestPerformanceTracker tracker : trackers.values()) {
                if (tracker.getAndResetIsUpdated()) {
                    tracker.writeStatsToFile(currentTimestamp, dateString);

                    globalIntervalOperationCount += tracker.getIntervalOperationCount();
                    globalOperationsCount += tracker.getTotalOperationCount();
                    globalIntervalThroughput += tracker.getIntervalThroughput();
                }
            }

            // global performance stats
            globalPerformanceLogWriter.write(
                    currentTimestamp,
                    dateString,
                    globalOperationsCount,
                    globalIntervalOperationCount,
                    globalIntervalThroughput,
                    trackers.size(),
                    testContainers.size());
        }
    }
}
