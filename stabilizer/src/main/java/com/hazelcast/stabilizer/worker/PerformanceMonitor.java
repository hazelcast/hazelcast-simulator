package com.hazelcast.stabilizer.worker;

import com.hazelcast.stabilizer.test.TestContext;
import org.apache.log4j.Logger;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;

import static com.hazelcast.stabilizer.utils.CommonUtils.fillString;
import static com.hazelcast.stabilizer.utils.CommonUtils.formatDouble;
import static com.hazelcast.stabilizer.utils.CommonUtils.formatLong;
import static com.hazelcast.stabilizer.utils.FileUtils.appendText;
import static java.lang.String.format;

class PerformanceMonitor extends Thread {
    private static final Logger log = Logger.getLogger(PerformanceMonitor.class);

    private final File globalPerformanceFile = new File("performance.txt");
    private final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
    private final HashMap<String, TestStats> testStats = new HashMap<String, TestStats>();

    private final Collection<TestContainer<TestContext>> testContainers;

    private long globalLastOpsCount = 0;
    private long globalLastTimeMillis = System.currentTimeMillis();

    public PerformanceMonitor(Collection<TestContainer<TestContext>> testContainers) {
        super("PerformanceMonitorThread");
        setDaemon(true);

        this.testContainers = testContainers;

        writeHeaderToFile(true, globalPerformanceFile);
    }

    @Override
    public void run() {
        while (true) {
            try {
                Thread.sleep(5000);
                writeStatsToFiles();
            } catch (Throwable t) {
                log.fatal("Failed to run performance monitor", t);
            }
        }
    }

    private void writeStatsToFiles() {
        String timestamp = simpleDateFormat.format(new Date());
        long currentTimeMillis = System.currentTimeMillis();

        long numberOfTests = 0;
        long globalDeltaOps = 0;
        long deltaTimeMillis = currentTimeMillis - globalLastTimeMillis;

        for (TestContainer testContainer : testContainers) {
            String testId = testContainer.getTestContext().getTestId();

            TestStats stats = testStats.get(testId);
            if (stats == null) {
                File testFile = new File("performance-" + (testId.isEmpty() ? "default" : testId) + ".txt");
                writeHeaderToFile(false, testFile);

                stats = new TestStats(testFile);
                testStats.put(testId, stats);
            }

            long currentOpsCount = getOpsCount(testContainer);
            long deltaOps = currentOpsCount - stats.lastOpsCount;
            double opsPerSecond = (deltaOps * 1000d) / deltaTimeMillis;

            stats.lastOpsCount = currentOpsCount;
            globalDeltaOps += deltaOps;
            numberOfTests++;

            writeStatsToFile(timestamp, currentOpsCount, deltaOps, opsPerSecond, -1, stats.performanceFile);
        }

        double globalOpsPerSecond = (globalDeltaOps * 1000d) / deltaTimeMillis;

        globalLastOpsCount += globalDeltaOps;
        globalLastTimeMillis = currentTimeMillis;

        writeStatsToFile(
                timestamp, globalLastOpsCount, globalDeltaOps, globalOpsPerSecond, numberOfTests, globalPerformanceFile
        );
    }

    private long getOpsCount(TestContainer container) {
        try {
            long operationCount = container.getOperationCount();
            if (operationCount > 0) {
                return operationCount;
            }
        } catch (Throwable ignored) {
        }

        return 0;
    }

    private void writeHeaderToFile(boolean isGlobal, File file) {
        String columns = "Timestamp                      Ops (sum)        Ops (delta)                Ops/s";
        if (isGlobal) {
            columns += " Number of tests";
        }
        appendText(format("%s%n%s%n", columns, fillString(columns.length(), '-')), file);
    }

    private void writeStatsToFile(String timestamp, long opsSum, long opsDelta, double opsPerSecDelta,
                                  long numberOfTests, File file) {
        String dataString = "[%s] %s ops %s ops %s ops/s";
        if (numberOfTests != -1) {
            dataString += " %s";
        }
        appendText(format(dataString + "\n", timestamp, formatLong(opsSum, 14), formatLong(opsDelta, 14),
                formatDouble(opsPerSecDelta, 14), formatLong(numberOfTests, 15)), file);
    }

    private static class TestStats {
        public final File performanceFile;

        public long lastOpsCount = 0;

        public TestStats(File performanceFile) {
            this.performanceFile = performanceFile;
        }
    }
}
