package com.hazelcast.simulator.worker;

import com.hazelcast.simulator.protocol.connector.ServerConnector;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.operation.PerformanceStateOperation;
import com.hazelcast.simulator.protocol.operation.SimulatorOperation;
import com.hazelcast.simulator.protocol.operation.WorkerIsAliveOperation;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.worker.performance.PerformanceState;
import org.apache.log4j.Logger;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.simulator.utils.CommonUtils.fillString;
import static com.hazelcast.simulator.utils.CommonUtils.formatDouble;
import static com.hazelcast.simulator.utils.CommonUtils.formatLong;
import static com.hazelcast.simulator.utils.CommonUtils.sleepSeconds;
import static com.hazelcast.simulator.utils.FileUtils.appendText;
import static java.lang.String.format;

/**
 * Monitors the performance of all running tests on {@link MemberWorker} and {@link ClientWorker} instances.
 */
class WorkerPerformanceMonitor {

    private static final int DEFAULT_MONITORING_INTERVAL_SECONDS = 5;

    private final AtomicBoolean started = new AtomicBoolean();

    private final MonitorThread thread;

    public WorkerPerformanceMonitor(ServerConnector serverConnector, Collection<TestContainer<TestContext>> testContainers) {
        this(serverConnector, testContainers, DEFAULT_MONITORING_INTERVAL_SECONDS);
    }

    WorkerPerformanceMonitor(ServerConnector serverConnector, Collection<TestContainer<TestContext>> testContainers,
                             int intervalSeconds) {
        this.thread = new MonitorThread(serverConnector, testContainers, intervalSeconds);
    }

    public boolean start() {
        if (!started.compareAndSet(false, true)) {
            return false;
        }

        thread.start();
        return true;
    }

    public void shutdown() {
        thread.isRunning = false;
        thread.interrupt();
    }

    private static final class MonitorThread extends Thread {

        private static final Logger LOGGER = Logger.getLogger(MonitorThread.class);

        private final File globalPerformanceFile = new File("performance.txt");
        private final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
        private final Map<String, TestStats> testStats = new HashMap<String, TestStats>();

        private final ServerConnector serverConnector;
        private final Collection<TestContainer<TestContext>> testContainers;
        private final int intervalSeconds;

        private final SimulatorOperation keepAliveOperation;
        private final SimulatorAddress agentAddress;

        private long globalLastOpsCount;
        private long globalLastTimeMillis = System.currentTimeMillis();

        private volatile boolean isRunning = true;

        private MonitorThread(ServerConnector serverConnector, Collection<TestContainer<TestContext>> testContainers,
                              int intervalSeconds) {
            super("WorkerPerformanceMonitorThread");
            setDaemon(true);

            this.serverConnector = serverConnector;
            this.testContainers = testContainers;
            this.intervalSeconds = intervalSeconds;

            SimulatorAddress workerAddress = serverConnector.getAddress();
            this.keepAliveOperation = new WorkerIsAliveOperation(workerAddress);
            this.agentAddress = workerAddress.getParent();

            writeHeaderToFile(globalPerformanceFile, true);
        }

        @Override
        public void run() {
            long iteration = 0;
            while (isRunning) {
                try {
                    sleepSeconds(intervalSeconds);
                    updatePerformanceStates();
                    sendPerformanceState();
                    if (++iteration % intervalSeconds == 0) {
                        sendKeepAliveToAgent();
                        writeStatsToFiles();
                    }
                } catch (Throwable t) {
                    LOGGER.fatal("Failed to run performance monitor", t);
                }
            }
        }

        private void updatePerformanceStates() {
            for (TestContainer testContainer : testContainers) {
                String testId = testContainer.getTestContext().getTestId();
                PerformanceState performanceState = testContainer.getPerformanceState();
                if (performanceState == null || performanceState.getOperationCount() == PerformanceState.EMPTY_OPERATION_COUNT) {
                    // skip tests without performance counts
                    continue;
                }

                TestStats stats = testStats.get(testId);
                if (stats == null) {
                    File testFile = new File("performance-" + (testId.isEmpty() ? "default" : testId) + ".txt");
                    writeHeaderToFile(testFile, false);

                    stats = new TestStats(testFile);
                    testStats.put(testId, stats);
                }
                stats.performanceState = performanceState;
            }
        }

        private void sendPerformanceState() {
            PerformanceStateOperation operation = new PerformanceStateOperation(serverConnector.getAddress());
            for (Map.Entry<String, TestStats> statsEntry : testStats.entrySet()) {
                operation.addPerformanceState(statsEntry.getKey(), statsEntry.getValue().performanceState);
            }
            serverConnector.submit(SimulatorAddress.COORDINATOR, operation);
        }

        private void sendKeepAliveToAgent() {
            serverConnector.submit(agentAddress, keepAliveOperation);
        }

        @SuppressWarnings("checkstyle:magicnumber")
        private void writeStatsToFiles() {
            String timestamp = simpleDateFormat.format(new Date());
            long currentTimeMillis = System.currentTimeMillis();

            long numberOfTests = 0;
            long globalDeltaOps = 0;
            long deltaTimeMillis = currentTimeMillis - globalLastTimeMillis;

            // test performance stats
            for (TestStats stats : testStats.values()) {
                long currentOpsCount = stats.performanceState.getOperationCount();

                long deltaOps = currentOpsCount - stats.lastOpsCount;
                double opsPerSecond = (deltaOps * 1000d) / deltaTimeMillis;

                stats.lastOpsCount = currentOpsCount;
                globalDeltaOps += deltaOps;
                numberOfTests++;

                writeStatsToFile(stats.performanceFile, timestamp, currentOpsCount, deltaOps, opsPerSecond, 0, 0);
            }

            // global performance stats
            double globalOpsPerSecond = (globalDeltaOps * 1000d) / deltaTimeMillis;

            globalLastOpsCount += globalDeltaOps;
            globalLastTimeMillis = currentTimeMillis;

            if (numberOfTests > 0) {
                writeStatsToFile(globalPerformanceFile, timestamp, globalLastOpsCount, globalDeltaOps, globalOpsPerSecond,
                        numberOfTests, testContainers.size());
            }
        }

        private void writeHeaderToFile(File file, boolean isGlobal) {
            String columns = "Timestamp                      Ops (sum)        Ops (delta)                Ops/s";
            if (isGlobal) {
                columns += " Number of tests";
            }
            appendText(format("%s%n%s%n", columns, fillString(columns.length(), '-')), file);
        }

        @SuppressWarnings("checkstyle:magicnumber")
        private void writeStatsToFile(File file, String timestamp, long opsSum, long opsDelta, double opsPerSecDelta,
                                      long numberOfTests, long totalTests) {
            String dataString = "[%s] %s ops %s ops %s ops/s";
            if (totalTests > 0) {
                dataString += " %s/%s";
            }
            int fieldLength = 1;
            if (totalTests >= 100) {
                fieldLength = 3;
            } else if (totalTests >= 10) {
                fieldLength = 2;
            }
            appendText(format(dataString + "\n", timestamp, formatLong(opsSum, 14), formatLong(opsDelta, 14),
                    formatDouble(opsPerSecDelta, 14), formatLong(numberOfTests, 14 - fieldLength),
                    formatLong(totalTests, fieldLength)), file);
        }

        private static final class TestStats {

            private final File performanceFile;
            private PerformanceState performanceState;
            private long lastOpsCount;

            private TestStats(File performanceFile) {
                this.performanceFile = performanceFile;
            }
        }
    }
}
