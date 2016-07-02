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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.simulator.utils.CommonUtils.sleepNanos;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Monitors the performance of all running Simulator Tests on {@link com.hazelcast.simulator.worker.MemberWorker}
 * and {@link com.hazelcast.simulator.worker.ClientWorker} instances.
 */
public class WorkerPerformanceMonitor {

    private static final int SHUTDOWN_TIMEOUT_SECONDS = 10;
    private static final long WAIT_FOR_TEST_CONTAINERS_DELAY_NANOS = TimeUnit.MILLISECONDS.toNanos(100);
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
                LOGGER.fatal(e);
            }
        });
    }

    public void start() {
        thread.start();
    }

    public void shutdown() throws InterruptedException {
        if (!shutdown.compareAndSet(false, true)) {
            return;
        }

        thread.join(MINUTES.toMillis(SHUTDOWN_TIMEOUT_SECONDS));
    }

    /**
     * Thread to monitor the performance of Simulator Tests.
     * <p>
     * Iterates over all {@link TestContainer} to retrieve performance values from all {@link Probe} instances.
     * Sends performance numbers as {@link PerformanceState} to the Coordinator.
     * Writes performance stats to files.
     * <p>
     * Holds one {@link TestPerformanceTracker} instance per Simulator Test.
     */
    private final class WorkerPerformanceMonitorThread extends Thread {

        private final PerformanceStatsWriter globalPerformanceStatsWriter;
        private final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
        private final Map<String, MonitoredTest> tests = new ConcurrentHashMap<String, MonitoredTest>();
        private final ServerConnector serverConnector;
        private final Collection<TestContainer> testContainers;
        private final long intervalNanos;

        private WorkerPerformanceMonitorThread(ServerConnector serverConnector,
                                               Collection<TestContainer> testContainers,
                                               long intervalNanos) {
            super("WorkerPerformanceMonitor");
            this.serverConnector = serverConnector;
            this.testContainers = testContainers;
            this.intervalNanos = intervalNanos;
            this.globalPerformanceStatsWriter = new PerformanceStatsWriter(new File("performance.csv"));
        }

        @Override
        public void run() {
            while (!shutdown.get()) {
                long startedNanos = System.nanoTime();
                long currentTimestamp = System.currentTimeMillis();

                boolean runningTestFound = refreshTests(currentTimestamp);
                updateTrackers(currentTimestamp);
                sendPerformanceStates();
                writeStatsToFiles(currentTimestamp);
                purgeDeadTests(currentTimestamp);

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
            sendTestHistograms();
        }

        private void sendTestHistograms() {
            for (Map.Entry<String, MonitoredTest> entry : tests.entrySet()) {
                String testId = entry.getKey();
                TestPerformanceTracker tracker = entry.getValue().tracker;

                Map<String, String> histograms = tracker.aggregateIntervalHistograms();
                if (!histograms.isEmpty()) {
                    TestHistogramOperation operation = new TestHistogramOperation(testId, histograms);
                    serverConnector.write(SimulatorAddress.COORDINATOR, operation);
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
                MonitoredTest test = tests.get(testId);
                if (test == null) {
                    test = new MonitoredTest(testContainer);
                    tests.put(testId, test);
                }

                // we set the lastSeen timestamp, so we can easily purge dead tests
                test.lastSeen = currentTimestamp;
                runningTestFound = true;
            }

            return runningTestFound;
        }

        private void updateTrackers(long currentTimestamp) {
            for (MonitoredTest test : tests.values()) {
                updateTrackers(currentTimestamp, test);
            }
        }

        // we remove every MonitoredTest that doesn't have the desired timestamp
        private void purgeDeadTests(long currentTimestamp) {
            for (MonitoredTest test : tests.values()) {
                // purge the testData if it wasn't seen in the current run
                if (test.lastSeen != currentTimestamp) {
                    tests.remove(test.testId);
                }
            }
        }

        private void updateTrackers(long currentTimestamp, MonitoredTest test) {
            TestContainer testContainer = test.testContainer;
            Map<String, Probe> probeMap = testContainer.getProbeMap();
            Map<String, Histogram> intervalHistograms = new HashMap<String, Histogram>(probeMap.size());

            long intervalPercentileLatency = Long.MIN_VALUE;
            double intervalAvgLatency = Long.MIN_VALUE;
            long intervalMaxLatency = Long.MIN_VALUE;
            long intervalOperationalCount = 0;

//            for (Map.Entry<String, Probe> entry : probeMap.entrySet()) {
//                String probeName = entry.getKey();
//                Probe probe = entry.getValue();
//
//                if (probe instanceof HdrProbe) {
//                    HdrProbe hdrProbe = (HdrProbe) probe;
//                    Histogram intervalHistogram = hdrProbe.getIntervalHistogram();
//                    intervalHistograms.put(probeName, intervalHistogram);
//
//                    long percentileValue = intervalHistogram.getValueAtPercentile(INTERVAL_LATENCY_PERCENTILE);
//                    if (percentileValue > intervalPercentileLatency) {
//                        intervalPercentileLatency = percentileValue;
//                    }
//                    double avgValue = intervalHistogram.getMean();
//                    if (avgValue > intervalAvgLatency) {
//                        intervalAvgLatency = avgValue;
//                    }
//                    long maxValue = intervalHistogram.getMaxValue();
//                    if (maxValue > intervalMaxLatency) {
//                        intervalMaxLatency = maxValue;
//                    }
//                    if (probe.isPartOfTotalThroughput()) {
//                        intervalOperationalCount += intervalHistogram.getTotalCount();
//                    }
//                } else {
//                    intervalPercentileLatency = -1;
//                    intervalAvgLatency = -1;
//                    intervalMaxLatency = -1;
//
//                    if (probe.isPartOfTotalThroughput()) {
//                        AtomicLong previous = test.getOrCreatePrevious(probe);
//
//                        long current = probe.get();
//                        long delta = current - previous.get();
//                        previous.set(current);
//
//                        intervalOperationalCount += delta;
//                    }
//                }
//            }

            intervalPercentileLatency = -1;
            intervalAvgLatency = -1;
            intervalMaxLatency = -1;

            long currentIterations = testContainer.iteration();
            long previousIterations = test.iterationPrevious;
            test.iterationPrevious = currentIterations;

            intervalOperationalCount = currentIterations - previousIterations;

            test.tracker.update(intervalHistograms, intervalPercentileLatency, intervalAvgLatency, intervalMaxLatency,
                    intervalOperationalCount, currentTimestamp);
        }

        private void sendPerformanceStates() {
            PerformanceStateOperation operation = new PerformanceStateOperation();

            for (MonitoredTest test : tests.values()) {
                if (test.tracker.isUpdated()) {
                    operation.addPerformanceState(test.testId, test.tracker.createPerformanceState());
                }
            }

            if (operation.getPerformanceStates().size() > 0) {
                serverConnector.submit(SimulatorAddress.COORDINATOR, operation);
            }
        }

        private void writeStatsToFiles(long currentTimestamp) {
            if (tests.isEmpty()) {
                return;
            }

            String dateString = simpleDateFormat.format(new Date(currentTimestamp));
            long globalIntervalOperationCount = 0;
            long globalOperationsCount = 0;
            double globalIntervalThroughput = 0;

            // performance stats per Simulator Test
            for (MonitoredTest test : tests.values()) {
                TestPerformanceTracker tracker = test.tracker;
                if (tracker.getAndResetIsUpdated()) {
                    tracker.writeStatsToFile(currentTimestamp, dateString);

                    globalIntervalOperationCount += tracker.getIntervalOperationCount();
                    globalOperationsCount += tracker.getTotalOperationCount();
                    globalIntervalThroughput += tracker.getIntervalThroughput();
                }
            }

            // global performance stats
            globalPerformanceStatsWriter.write(
                    currentTimestamp,
                    dateString,
                    globalOperationsCount,
                    globalIntervalOperationCount,
                    globalIntervalThroughput,
                    tests.size(),
                    testContainers.size());
        }
    }

    /**
     * The Monitored test is wrapper around a {@link TestContainer} where all kinds of {@link WorkerPerformanceMonitor}
     * specific functionality for a given test can be added.
     */
    private static class MonitoredTest {

        private long iterationPrevious;
        private final TestPerformanceTracker tracker;
        private final Map<Probe, AtomicLong> previousProbeValues = new HashMap<Probe, AtomicLong>();
        private final TestContainer testContainer;
        private final String testId;

        // used to determine if the MonitoredTest can be deleted
        private long lastSeen;

        MonitoredTest(TestContainer testContainer) {
            this.testContainer = testContainer;
            this.tracker = new TestPerformanceTracker(
                    testContainer.getTestContext().getTestId(),
                    testContainer.getProbeMap().keySet(),
                    testContainer.getTestStartedTimestamp());
            this.testId = testContainer.getTestContext().getTestId();
        }

        AtomicLong getOrCreatePrevious(Probe probe) {
            AtomicLong previous = previousProbeValues.get(probe);
            if (previous == null) {
                previous = new AtomicLong();
                previousProbeValues.put(probe, previous);
            }
            return previous;
        }
    }
}
