package com.hazelcast.simulator.worker.performance;

import com.hazelcast.simulator.protocol.connector.ServerConnector;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.operation.PerformanceStateOperation;
import com.hazelcast.simulator.utils.EmptyStatement;
import com.hazelcast.simulator.worker.ClientWorker;
import com.hazelcast.simulator.worker.MemberWorker;
import com.hazelcast.simulator.worker.TestContainer;
import org.HdrHistogram.Histogram;
import org.apache.log4j.Logger;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.simulator.utils.CommonUtils.sleepNanos;
import static com.hazelcast.simulator.worker.performance.PerformanceState.EMPTY_OPERATION_COUNT;
import static com.hazelcast.simulator.worker.performance.PerformanceUtils.ONE_SECOND_IN_MILLIS;
import static com.hazelcast.simulator.worker.performance.PerformanceUtils.writeThroughputHeader;
import static com.hazelcast.simulator.worker.performance.PerformanceUtils.writeThroughputStats;

/**
 * Monitors the performance of all running tests on {@link MemberWorker} and {@link ClientWorker} instances.
 */
public class WorkerPerformanceMonitor {

    private static final int DEFAULT_MONITORING_INTERVAL_SECONDS = 1;

    private final AtomicBoolean started = new AtomicBoolean();

    private final MonitorThread thread;

    public WorkerPerformanceMonitor(ServerConnector serverConnector, Collection<TestContainer> testContainers) {
        this.thread = new MonitorThread(serverConnector, testContainers);
    }

    public boolean start() {
        if (!started.compareAndSet(false, true)) {
            return false;
        }

        thread.start();
        return true;
    }

    public void shutdown() {
        try {
            thread.isRunning = false;
            thread.interrupt();
            thread.join();
        } catch (InterruptedException e) {
            EmptyStatement.ignore(e);
        }
    }

    private static final class MonitorThread extends Thread {

        private static final Logger LOGGER = Logger.getLogger(MonitorThread.class);

        private final File globalThroughputFile = new File("throughput.txt");
        private final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
        private final Map<String, PerformanceTracker> trackerMap = new HashMap<String, PerformanceTracker>();

        private final ServerConnector serverConnector;
        private final Collection<TestContainer> testContainers;
        private final long intervalNanos;

        private long globalLastOpsCount;
        private long globalLastTimestamp;

        private long startedTimestamp;
        private long lastTimestamp;

        private volatile boolean isRunning = true;

        private MonitorThread(ServerConnector serverConnector, Collection<TestContainer> testContainers) {
            super("WorkerPerformanceMonitorThread");
            setDaemon(true);

            this.serverConnector = serverConnector;
            this.testContainers = testContainers;
            this.intervalNanos = TimeUnit.SECONDS.toNanos(DEFAULT_MONITORING_INTERVAL_SECONDS);

            writeThroughputHeader(globalThroughputFile, true);
        }

        @Override
        public void start() {
            startedTimestamp = System.currentTimeMillis();
            globalLastTimestamp = startedTimestamp;
            lastTimestamp = startedTimestamp;
            super.start();
        }

        @Override
        public void run() {
            while (isRunning) {
                long started = System.nanoTime();

                try {
                    long currentTimestamp = System.currentTimeMillis();
                    updatePerformanceStates(currentTimestamp);
                    sendPerformanceState();
                    writeStatsToFiles(currentTimestamp);
                } catch (Exception e) {
                    LOGGER.fatal("Exception in WorkerPerformanceMonitorThread", e);
                }

                long elapsedNanos = System.nanoTime() - started;
                if (intervalNanos > elapsedNanos) {
                    sleepNanos(intervalNanos - elapsedNanos);
                } else {
                    LOGGER.warn("WorkerPerformanceMonitorThread ran for " + TimeUnit.NANOSECONDS.toMillis(elapsedNanos) + " ms");
                }
            }
        }

        private void updatePerformanceStates(long currentTimestamp) {
            long totalTimeDelta = currentTimestamp - startedTimestamp;
            long intervalTimeDelta = currentTimestamp - lastTimestamp;

            for (TestContainer testContainer : testContainers) {
                if (!testContainer.isRunning()) {
                    continue;
                }

                long currentOperationalCount = testContainer.getOperationCount();
                Map<String, Histogram> intervalHistograms = testContainer.getIntervalHistograms();
                if (currentOperationalCount == EMPTY_OPERATION_COUNT) {
                    continue;
                }

                String testId = testContainer.getTestContext().getTestId();
                PerformanceTracker tracker = getOrCreatePerformanceTracker(testContainer, testId);
                tracker.update(totalTimeDelta, intervalTimeDelta, currentOperationalCount, intervalHistograms);
            }

            lastTimestamp = currentTimestamp;
        }

        private PerformanceTracker getOrCreatePerformanceTracker(TestContainer testContainer, String testId) {
            PerformanceTracker tracker = trackerMap.get(testId);
            if (tracker == null) {
                tracker = new PerformanceTracker(testId, testContainer.getProbeNames(), startedTimestamp);
                trackerMap.put(testId, tracker);
            }
            return tracker;
        }

        private void sendPerformanceState() {
            PerformanceStateOperation operation = new PerformanceStateOperation(serverConnector.getAddress());
            for (Map.Entry<String, PerformanceTracker> statsEntry : trackerMap.entrySet()) {
                String testId = statsEntry.getKey();
                PerformanceTracker stats = statsEntry.getValue();
                operation.addPerformanceState(testId, stats.createPerformanceState());
            }
            serverConnector.submit(SimulatorAddress.COORDINATOR, operation);
        }

        private void writeStatsToFiles(long currentTimestamp) {
            String timestamp = simpleDateFormat.format(new Date(currentTimestamp));

            long runningTests = 0;
            long globalDeltaOps = 0;
            long deltaTimeMillis = currentTimestamp - globalLastTimestamp;

            // test performance stats
            for (PerformanceTracker stats : trackerMap.values()) {
                globalDeltaOps += stats.getOperationCountDelta();
                runningTests++;

                stats.writeStatsToFile(timestamp);
            }

            // global performance stats
            double globalOpsPerSecond = (globalDeltaOps * ONE_SECOND_IN_MILLIS) / (double) deltaTimeMillis;

            globalLastOpsCount += globalDeltaOps;
            globalLastTimestamp = currentTimestamp;

            if (runningTests > 0) {
                writeThroughputStats(globalThroughputFile, timestamp, globalLastOpsCount, globalDeltaOps, globalOpsPerSecond,
                        runningTests, testContainers.size());
            }
        }
    }
}
