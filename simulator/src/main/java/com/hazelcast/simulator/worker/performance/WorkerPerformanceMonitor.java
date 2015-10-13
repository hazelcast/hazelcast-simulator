package com.hazelcast.simulator.worker.performance;

import com.hazelcast.simulator.probes.Probe;
import com.hazelcast.simulator.protocol.connector.ServerConnector;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.operation.PerformanceStateOperation;
import com.hazelcast.simulator.protocol.operation.TestHistogramOperation;
import com.hazelcast.simulator.worker.TestContainer;
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
 * Monitors the performance of all running tests on {@link com.hazelcast.simulator.worker.MemberWorker}
 * and {@link com.hazelcast.simulator.worker.ClientWorker} instances.
 */
public class WorkerPerformanceMonitor {

    private final AtomicBoolean started = new AtomicBoolean();

    private final MonitorThread thread;

    public WorkerPerformanceMonitor(ServerConnector serverConnector, Collection<TestContainer> testContainers,
                                    int workerPerformanceMonitorIntervalSeconds) {
        this.thread = new MonitorThread(serverConnector, testContainers, workerPerformanceMonitorIntervalSeconds);
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

    private static final class MonitorThread extends Thread {

        private static final Logger LOGGER = Logger.getLogger(MonitorThread.class);

        private final File globalThroughputFile = new File("throughput.txt");
        private final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
        private final Map<String, PerformanceTracker> trackerMap = new HashMap<String, PerformanceTracker>();

        private final ServerConnector serverConnector;
        private final Collection<TestContainer> testContainers;
        private final long intervalNanos;

        private volatile boolean isRunning = true;

        private MonitorThread(ServerConnector serverConnector, Collection<TestContainer> testContainers,
                              int workerPerformanceMonitorIntervalSeconds) {
            super("WorkerPerformanceMonitorThread");
            setDaemon(true);

            this.serverConnector = serverConnector;
            this.testContainers = testContainers;
            this.intervalNanos = TimeUnit.SECONDS.toNanos(workerPerformanceMonitorIntervalSeconds);

            writeThroughputHeader(globalThroughputFile, true);
        }

        @Override
        public void run() {
            while (isRunning) {
                long startedNanos = System.nanoTime();
                long currentTimestamp = System.currentTimeMillis();

                updatePerformanceStates(currentTimestamp);
                sendPerformanceStates();
                writeStatsToFiles(currentTimestamp);

                long elapsedNanos = System.nanoTime() - startedNanos;
                if (intervalNanos > elapsedNanos) {
                    sleepNanos(intervalNanos - elapsedNanos);
                } else {
                    LOGGER.warn("WorkerPerformanceMonitorThread.run() took " + NANOSECONDS.toMillis(elapsedNanos) + " ms");
                }
            }
        }

        public void sendTestHistograms() {
            for (Map.Entry<String, PerformanceTracker> trackerEntry : trackerMap.entrySet()) {
                String testId = trackerEntry.getKey();
                PerformanceTracker tracker = trackerEntry.getValue();

                Map<String, String> histograms = tracker.aggregateIntervalHistograms(testId);
                if (!histograms.isEmpty()) {
                    TestHistogramOperation operation = new TestHistogramOperation(serverConnector.getAddress(), testId,
                            histograms);
                    serverConnector.write(SimulatorAddress.COORDINATOR, operation);
                }
            }
        }

        private void updatePerformanceStates(long currentTimestamp) {
            for (TestContainer testContainer : testContainers) {
                if (!testContainer.isRunning()) {
                    continue;
                }
                Map<String, Probe> probeMap = testContainer.getProbeMap();
                Map<String, Histogram> intervalHistograms = new HashMap<String, Histogram>(probeMap.size());

                long intervalPercentileLatency = Long.MIN_VALUE;
                long intervalMaxLatency = Long.MIN_VALUE;
                long intervalOperationalCount = 0;

                for (Map.Entry<String, Probe> entry : probeMap.entrySet()) {
                    Probe probe = entry.getValue();
                    Histogram intervalHistogram = probe.getIntervalHistogram();
                    intervalHistograms.put(entry.getKey(), intervalHistogram);

                    long percentileValue = intervalHistogram.getValueAtPercentile(INTERVAL_LATENCY_PERCENTILE);
                    if (percentileValue > intervalPercentileLatency) {
                        intervalPercentileLatency = percentileValue;
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

                tracker.update(intervalHistograms, intervalPercentileLatency, intervalMaxLatency,
                        intervalOperationalCount, currentTimestamp);
            }
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
            PerformanceStateOperation operation = new PerformanceStateOperation(serverConnector.getAddress());
            for (Map.Entry<String, PerformanceTracker> trackerEntry : trackerMap.entrySet()) {
                String testId = trackerEntry.getKey();
                PerformanceTracker stats = trackerEntry.getValue();
                operation.addPerformanceState(testId, stats.createPerformanceState());
            }
            serverConnector.submit(SimulatorAddress.COORDINATOR, operation);
        }

        private void writeStatsToFiles(long currentTimestamp) {
            if (trackerMap.isEmpty()) {
                return;
            }

            String dateString = simpleDateFormat.format(new Date(currentTimestamp));
            long globalIntervalOperationCount = 0;
            long globalOperationsCount = 0;
            double globalIntervalThroughput = 0;

            // test performance stats
            for (PerformanceTracker stats : trackerMap.values()) {
                stats.writeStatsToFile(dateString);

                globalIntervalOperationCount += stats.getIntervalOperationCount();
                globalOperationsCount += stats.getTotalOperationCount();
                globalIntervalThroughput += stats.getIntervalThroughput();
            }

            // global performance stats
            writeThroughputStats(globalThroughputFile, dateString, globalOperationsCount, globalIntervalOperationCount,
                    globalIntervalThroughput, trackerMap.size(), testContainers.size());
        }
    }
}
