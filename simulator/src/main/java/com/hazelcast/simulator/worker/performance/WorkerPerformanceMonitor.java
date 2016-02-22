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
import com.hazelcast.simulator.protocol.connector.ServerConnector;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.operation.PerformanceStateOperation;
import com.hazelcast.simulator.protocol.operation.TestHistogramOperation;
import com.hazelcast.simulator.test.TestContainer;
import org.HdrHistogram.Histogram;
import org.apache.log4j.Logger;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.simulator.utils.CommonUtils.joinThread;
import static com.hazelcast.simulator.utils.CommonUtils.sleepNanos;
import static com.hazelcast.simulator.worker.performance.PerformanceState.INTERVAL_LATENCY_PERCENTILE;
import static com.hazelcast.simulator.worker.performance.PerformanceUtils.writeThroughputHeader;
import static com.hazelcast.simulator.worker.performance.PerformanceUtils.writeThroughputStats;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Monitors the performance of all running Simulator Tests on {@link com.hazelcast.simulator.worker.MemberWorker}
 * and {@link com.hazelcast.simulator.worker.ClientWorker} instances.
 */
public class WorkerPerformanceMonitor {

    private final AtomicBoolean started = new AtomicBoolean();

    private final MonitorThread thread;

    public WorkerPerformanceMonitor(ServerConnector serverConnector, Collection<TestContainer> testContainers,
                                    int workerPerformanceMonitorInterval, TimeUnit workerPerformanceIntervalTimeUnit) {
        long intervalNanos = workerPerformanceIntervalTimeUnit.toNanos(workerPerformanceMonitorInterval);
        this.thread = new MonitorThread(serverConnector, testContainers, intervalNanos);
    }

    public boolean start() {
        if (!started.compareAndSet(false, true)) {
            return false;
        }

        thread.start();
        return true;
    }

    public void shutdown() {
        thread.sendTestHistograms();

        thread.isRunning = false;
        thread.interrupt();
        joinThread(thread);
    }

    /**
     * Internal thread to monitor the performance of Simulator Tests.
     *
     * Iterates over all {@link TestContainer} to retrieve performance values from all {@link Probe} instances.
     * Sends performance numbers as {@link PerformanceState} to the Coordinator.
     * Writes performance stats to files.
     *
     * Holds one {@link PerformanceTracker} instance per Simulator Test.
     */
    private static final class MonitorThread extends Thread {

        private static final long WAIT_FOR_TEST_CONTAINERS_DELAY_NANOS = TimeUnit.MILLISECONDS.toNanos(100);

        private static final Logger LOGGER = Logger.getLogger(MonitorThread.class);

        private final File globalThroughputFile = new File("throughput.txt");
        private final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
        private final Map<String, PerformanceTracker> trackerMap = new HashMap<String, PerformanceTracker>();

        private final ServerConnector serverConnector;
        private final Collection<TestContainer> testContainers;
        private final long intervalNanos;

        private volatile boolean isRunning = true;

        private MonitorThread(ServerConnector serverConnector, Collection<TestContainer> testContainers, long intervalNanos) {
            super("WorkerPerformanceMonitorThread");
            setDaemon(true);

            this.serverConnector = serverConnector;
            this.testContainers = testContainers;
            this.intervalNanos = intervalNanos;

            writeThroughputHeader(globalThroughputFile, true);
        }

        @Override
        public void run() {
            while (isRunning) {
                long startedNanos = System.nanoTime();
                long currentTimestamp = System.currentTimeMillis();

                boolean runningTestContainerFound = updatePerformanceStates(currentTimestamp);
                sendPerformanceStates();
                writeStatsToFiles(currentTimestamp);

                long elapsedNanos = System.nanoTime() - startedNanos;
                if (intervalNanos > elapsedNanos) {
                    if (runningTestContainerFound) {
                        sleepNanos(intervalNanos - elapsedNanos);
                    } else {
                        sleepNanos(WAIT_FOR_TEST_CONTAINERS_DELAY_NANOS - elapsedNanos);
                    }
                } else {
                    LOGGER.warn("WorkerPerformanceMonitorThread.run() took " + NANOSECONDS.toMillis(elapsedNanos) + " ms");
                }
            }
        }

        private void sendTestHistograms() {
            for (Map.Entry<String, PerformanceTracker> trackerEntry : trackerMap.entrySet()) {
                String testId = trackerEntry.getKey();
                PerformanceTracker tracker = trackerEntry.getValue();

                Map<String, String> histograms = tracker.aggregateIntervalHistograms(testId);
                if (!histograms.isEmpty()) {
                    TestHistogramOperation operation = new TestHistogramOperation(testId, histograms);
                    serverConnector.write(SimulatorAddress.COORDINATOR, operation);
                }
            }
        }

        private boolean updatePerformanceStates(long currentTimestamp) {
            boolean runningTestContainerFound = false;
            for (TestContainer testContainer : testContainers) {
                if (!testContainer.isRunning()) {
                    continue;
                }
                runningTestContainerFound = true;

                Map<String, Probe> probeMap = testContainer.getProbeMap();
                Map<String, Histogram> intervalHistograms = new HashMap<String, Histogram>(probeMap.size());

                long intervalPercentileLatency = Long.MIN_VALUE;
                double intervalAvgLatency = Long.MIN_VALUE;
                long intervalMaxLatency = Long.MIN_VALUE;
                long intervalOperationalCount = 0;

                for (Map.Entry<String, Probe> entry : probeMap.entrySet()) {
                    String probeName = entry.getKey();
                    Probe probe = entry.getValue();

                    Histogram intervalHistogram = probe.getIntervalHistogram();
                    intervalHistograms.put(probeName, intervalHistogram);

                    long percentileValue = intervalHistogram.getValueAtPercentile(INTERVAL_LATENCY_PERCENTILE);
                    if (percentileValue > intervalPercentileLatency) {
                        intervalPercentileLatency = percentileValue;
                    }
                    double avgValue = intervalHistogram.getMean();
                    if (avgValue > intervalAvgLatency) {
                        intervalAvgLatency = avgValue;
                    }
                    long maxValue = intervalHistogram.getMaxValue();
                    if (maxValue > intervalMaxLatency) {
                        intervalMaxLatency = maxValue;
                    }
                    if (probe.isThroughputProbe()) {
                        intervalOperationalCount += intervalHistogram.getTotalCount();
                    }
                }

                String testId = testContainer.getTestContext().getTestId();
                PerformanceTracker tracker = getOrCreatePerformanceTracker(testId, testContainer);
                tracker.update(intervalHistograms, intervalPercentileLatency, intervalAvgLatency, intervalMaxLatency,
                        intervalOperationalCount, currentTimestamp);
            }
            return runningTestContainerFound;
        }

        private PerformanceTracker getOrCreatePerformanceTracker(String testId, TestContainer testContainer) {
            PerformanceTracker tracker = trackerMap.get(testId);
            if (tracker == null) {
                Set<String> probeNames = testContainer.getProbeMap().keySet();
                tracker = new PerformanceTracker(testId, probeNames, testContainer.getTestStartedTimestamp());
                trackerMap.put(testId, tracker);
            }
            return tracker;
        }

        private void sendPerformanceStates() {
            PerformanceStateOperation operation = new PerformanceStateOperation();
            for (Map.Entry<String, PerformanceTracker> trackerEntry : trackerMap.entrySet()) {
                PerformanceTracker tracker = trackerEntry.getValue();
                if (tracker.isUpdated()) {
                    String testId = trackerEntry.getKey();
                    operation.addPerformanceState(testId, tracker.createPerformanceState());
                }
            }
            if (operation.getPerformanceStates().size() > 0) {
                serverConnector.submit(SimulatorAddress.COORDINATOR, operation);
            }
        }

        private void writeStatsToFiles(long currentTimestamp) {
            if (trackerMap.isEmpty()) {
                return;
            }

            String dateString = simpleDateFormat.format(new Date(currentTimestamp));
            long globalIntervalOperationCount = 0;
            long globalOperationsCount = 0;
            double globalIntervalThroughput = 0;

            // performance stats per Simulator Test
            for (PerformanceTracker tracker : trackerMap.values()) {
                if (tracker.getAndResetIsUpdated()) {
                    tracker.writeStatsToFile(dateString);

                    globalIntervalOperationCount += tracker.getIntervalOperationCount();
                    globalOperationsCount += tracker.getTotalOperationCount();
                    globalIntervalThroughput += tracker.getIntervalThroughput();
                }
            }

            // global performance stats
            writeThroughputStats(globalThroughputFile, dateString, globalOperationsCount, globalIntervalOperationCount,
                    globalIntervalThroughput, trackerMap.size(), testContainers.size());
        }
    }
}
