package com.hazelcast.simulator.worker.performance;

import com.hazelcast.simulator.test.TestException;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.HistogramLogReader;
import org.HdrHistogram.HistogramLogWriter;

import javax.xml.bind.DatatypeConverter;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.Deflater;

import static com.hazelcast.simulator.probes.probes.impl.ProbeImpl.LATENCY_PRECISION;
import static com.hazelcast.simulator.probes.probes.impl.ProbeImpl.MAXIMUM_LATENCY;
import static com.hazelcast.simulator.worker.performance.PerformanceState.INTERVAL_LATENCY_PERCENTILE;
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
        String testName = getTestName(testId);

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
        long intervalPercentileLatency = Long.MIN_VALUE;
        long intervalMaxLatency = Long.MIN_VALUE;
        for (Histogram histogram : intervalHistogramMap.values()) {
            long percentileValue = histogram.getValueAtPercentile(INTERVAL_LATENCY_PERCENTILE);
            if (percentileValue > intervalPercentileLatency) {
                intervalPercentileLatency = percentileValue;
            }
            long maxValue = histogram.getMaxValue();
            if (maxValue > intervalMaxLatency) {
                intervalMaxLatency = maxValue;
            }
        }

        return new PerformanceState(lastOperationCount, intervalThroughput, totalThroughput,
                intervalPercentileLatency, intervalMaxLatency);
    }

    public Map<String, String> aggregateIntervalHistograms(String testId) {
        Map<String, String> probeResults = new HashMap<String, String>();

        String testName = getTestName(testId);
        HistogramLogWriter histogramLogWriter = createHistogramLogWriter(testName, "aggregated", 0);
        for (Map.Entry<String, Histogram> histogramEntry : intervalHistogramMap.entrySet()) {
            String probeName = histogramEntry.getKey();
            HistogramLogReader histogramLogReader = createHistogramLogReader(testName, probeName);
            Histogram combined = new Histogram(MAXIMUM_LATENCY, LATENCY_PRECISION);

            Histogram histogram = (Histogram) histogramLogReader.nextIntervalHistogram();
            while (histogram != null) {
                combined.add(histogram);
                histogram = (Histogram) histogramLogReader.nextIntervalHistogram();
            }

            histogramLogWriter.outputComment("probeName=" + probeName);
            histogramLogWriter.outputIntervalHistogram(combined);

            String encodedHistogram = getEncodedHistogram(combined);
            probeResults.put(probeName, encodedHistogram);
        }

        return probeResults;
    }

    private static String getTestName(String testId) {
        return (testId.isEmpty() ? "default" : testId);
    }

    private static File getLatencyFile(String testName, String probeName) {
        return new File("latency-" + testName + "-" + probeName + ".txt");
    }

    private static HistogramLogWriter createHistogramLogWriter(String testName, String probeName, long baseTime) {
        try {
            File latencyFile = getLatencyFile(testName, probeName);
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

    private HistogramLogReader createHistogramLogReader(String testName, String probeName) {
        try {
            File latencyFile = getLatencyFile(testName, probeName);
            return new HistogramLogReader(latencyFile);
        } catch (IOException e) {
            throw new TestException("Could not initialize HistogramLogReader for test " + testName, e);
        }
    }

    private static String getEncodedHistogram(Histogram combined) {
        ByteBuffer targetBuffer = ByteBuffer.allocate(combined.getNeededByteBufferCapacity());
        int compressedLength = combined.encodeIntoCompressedByteBuffer(targetBuffer, Deflater.BEST_COMPRESSION);
        byte[] compressedArray = Arrays.copyOf(targetBuffer.array(), compressedLength);
        return DatatypeConverter.printBase64Binary(compressedArray);
    }
}
