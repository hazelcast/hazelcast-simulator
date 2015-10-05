package com.hazelcast.simulator.worker.performance;

import com.hazelcast.simulator.test.TestException;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.HistogramLogWriter;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.simulator.worker.performance.PerformanceUtils.ONE_SECOND_IN_MILLIS;
import static com.hazelcast.simulator.worker.performance.PerformanceUtils.writeThroughputHeader;
import static com.hazelcast.simulator.worker.performance.PerformanceUtils.writeThroughputStats;

final class PerformanceTracker {

    private final File throughputFile;
    private final Map<String, HistogramLogWriter> histogramLogWriterMap = new HashMap<String, HistogramLogWriter>();

    private Map<String, Histogram> intervalHistogramMap;

    private long operationCountDelta;
    private double intervalThroughput;
    private double totalThroughput;

    private long lastOperationCount;

    PerformanceTracker(String testId, Collection<String> probeNames, long baseTime) {
        String testName = (testId.isEmpty() ? "default" : testId);

        throughputFile = new File("throughput-" + testName + ".txt");
        writeThroughputHeader(throughputFile, false);

        for (String probeName : probeNames) {
            histogramLogWriterMap.put(probeName, createHistogramLogWriter(testName, probeName, baseTime));
        }
    }

    long getOperationCountDelta() {
        return operationCountDelta;
    }

    void update(long totalTimeDelta, long intervalTimeDelta, long currentOperationalCount,
                Map<String, Histogram> intervalHistograms) {
        operationCountDelta = currentOperationalCount - lastOperationCount;
        intervalThroughput = (operationCountDelta * ONE_SECOND_IN_MILLIS) / (double) intervalTimeDelta;
        totalThroughput = (currentOperationalCount * ONE_SECOND_IN_MILLIS / (double) totalTimeDelta);

        lastOperationCount = currentOperationalCount;

        intervalHistogramMap = intervalHistograms;
    }

    void writeStatsToFile(String timestamp) {
        writeThroughputStats(throughputFile, timestamp, lastOperationCount, operationCountDelta, intervalThroughput, 0, 0);

        for (Map.Entry<String, Histogram> histogramEntry : intervalHistogramMap.entrySet()) {
            String probeName = histogramEntry.getKey();
            HistogramLogWriter histogramLogWriter = histogramLogWriterMap.get(probeName);

            Histogram intervalHistogram = histogramEntry.getValue();
            histogramLogWriter.outputIntervalHistogram(intervalHistogram);
        }
    }

    PerformanceState createPerformanceState() {
        return new PerformanceState(lastOperationCount, intervalThroughput, totalThroughput);
    }

    private static HistogramLogWriter createHistogramLogWriter(String testName, String probeName, long baseTime) {
        try {
            File latencyFile = new File("latency-" + testName + "-" + probeName + ".txt");
            HistogramLogWriter histogramLogWriter = new HistogramLogWriter(latencyFile);
            histogramLogWriter.setBaseTime(baseTime);
            histogramLogWriter.outputComment("[Latency histograms for " + testName + "." + probeName + "]");
            histogramLogWriter.outputLogFormatVersion();
            histogramLogWriter.outputLegend();
            return histogramLogWriter;
        } catch (IOException e) {
            throw new TestException("Could not initialize HistogramLogWriter for test " + testName, e);
        }
    }
}
