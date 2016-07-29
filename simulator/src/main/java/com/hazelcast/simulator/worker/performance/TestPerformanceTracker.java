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

import com.hazelcast.simulator.test.TestContainer;
import com.hazelcast.simulator.test.TestException;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.HistogramLogReader;
import org.HdrHistogram.HistogramLogWriter;

import javax.xml.bind.DatatypeConverter;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.Deflater;

import static com.hazelcast.simulator.probes.impl.HdrProbe.LATENCY_PRECISION;
import static com.hazelcast.simulator.probes.impl.HdrProbe.MAXIMUM_LATENCY;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Tracks performance related values for a single Simulator Test.
 * <p>
 * Has methods to update the performance values and write them to files.
 * <p>
 * Holds a map of {@link Histogram} for each {@link com.hazelcast.simulator.probes.Probe} of a Simulator Test.
 */
final class TestPerformanceTracker {

    private static final long ONE_SECOND_IN_MILLIS = SECONDS.toMillis(1);

    final TestContainer testContainer;
    final String testId;

    long oldIterations;
    // used to determine if the TestPerformanceTracker can be deleted
    long lastSeen;

    private final Map<String, HistogramLogWriter> histogramLogWriterMap = new HashMap<String, HistogramLogWriter>();
    private final long testStartedTimestamp;
    private final PerformanceStatsWriter performanceStatsWriter;
    private long lastTimestamp;
    private Map<String, Histogram> intervalHistogramMap;
    private double intervalAvgLatency;
    private long intervalPercentileLatency;
    private long intervalMaxLatency;
    private long intervalOperationCount;
    private long totalOperationCount;
    private double intervalThroughput;
    private double totalThroughput;
    private boolean isUpdated;

    TestPerformanceTracker(TestContainer testContainer) {
        this.testContainer = testContainer;
        this.testId = testContainer.getTestCase().getId();
        this.testStartedTimestamp = testContainer.getTestStartedTimestamp();
        this.lastTimestamp = testStartedTimestamp;
        this.performanceStatsWriter = new PerformanceStatsWriter(new File("performance-" + testId + ".csv"));

        for (String probeName : testContainer.getProbeMap().keySet()) {
            HistogramLogWriter writer = createHistogramLogWriter(testId, probeName, testStartedTimestamp);
            histogramLogWriterMap.put(probeName, writer);
        }
    }

    long getIntervalOperationCount() {
        return intervalOperationCount;
    }

    long getTotalOperationCount() {
        return totalOperationCount;
    }

    double getIntervalThroughput() {
        return intervalThroughput;
    }

    boolean isUpdated() {
        return isUpdated;
    }

    boolean getAndResetIsUpdated() {
        boolean oldIsUpdated = isUpdated;
        isUpdated = false;
        return oldIsUpdated;
    }

    void update(Map<String, Histogram> intervalHistograms, long intervalPercentileLatency, double intervalAvgLatency,
                long intervalMaxLatency, long intervalOperationCount, long currentTimestamp) {
        this.intervalHistogramMap = intervalHistograms;

        this.intervalPercentileLatency = intervalPercentileLatency;
        this.intervalAvgLatency = intervalAvgLatency;
        this.intervalMaxLatency = intervalMaxLatency;

        this.intervalOperationCount = intervalOperationCount;
        this.totalOperationCount += intervalOperationCount;

        long intervalTimeDelta = currentTimestamp - lastTimestamp;
        long totalTimeDelta = currentTimestamp - testStartedTimestamp;

        this.intervalThroughput = (intervalOperationCount * ONE_SECOND_IN_MILLIS) / (double) intervalTimeDelta;
        this.totalThroughput = (totalOperationCount * ONE_SECOND_IN_MILLIS / (double) totalTimeDelta);

        this.lastTimestamp = currentTimestamp;
        this.isUpdated = true;
    }

    void writeStatsToFile(long epochTime, String timestamp) {
        performanceStatsWriter.write(
                epochTime,
                timestamp,
                totalOperationCount,
                intervalOperationCount,
                intervalThroughput,
                0,
                0);

        for (Map.Entry<String, Histogram> histogramEntry : intervalHistogramMap.entrySet()) {
            String probeName = histogramEntry.getKey();
            HistogramLogWriter writer = histogramLogWriterMap.get(probeName);

            Histogram intervalHistogram = histogramEntry.getValue();
            writer.outputIntervalHistogram(intervalHistogram);
        }
    }

    PerformanceState createPerformanceState() {
        return new PerformanceState(totalOperationCount, intervalThroughput, totalThroughput,
                intervalAvgLatency, intervalPercentileLatency, intervalMaxLatency);
    }

    Map<String, String> aggregateIntervalHistograms() {
        Map<String, String> probeResults = new HashMap<String, String>();

        HistogramLogWriter writer = createHistogramLogWriter(testId, "aggregated", 0);
        for (Map.Entry<String, Histogram> histogramEntry : intervalHistogramMap.entrySet()) {
            String probeName = histogramEntry.getKey();
            HistogramLogReader reader = createHistogramLogReader(testId, probeName);
            Histogram combined = new Histogram(MAXIMUM_LATENCY, LATENCY_PRECISION);

            Histogram histogram = (Histogram) reader.nextIntervalHistogram();
            while (histogram != null) {
                combined.add(histogram);
                histogram = (Histogram) reader.nextIntervalHistogram();
            }

            writer.outputComment("probeName=" + probeName);
            writer.outputIntervalHistogram(combined);

            String encodedHistogram = getEncodedHistogram(combined);
            probeResults.put(probeName, encodedHistogram);
        }

        return probeResults;
    }

    static HistogramLogWriter createHistogramLogWriter(String testId, String probeName, long baseTime) {
        try {
            File latencyFile = getLatencyFile(testId, probeName);
            HistogramLogWriter writer = new HistogramLogWriter(latencyFile);
            writer.outputStartTime(baseTime);
            writer.setBaseTime(baseTime);
            writer.outputComment("[Latency histograms for " + testId + '.' + probeName + ']');
            writer.outputLogFormatVersion();
            writer.outputLegend();
            return writer;
        } catch (IOException e) {
            throw new TestException("Could not initialize HistogramLogWriter for test " + testId, e);
        }
    }

    static HistogramLogReader createHistogramLogReader(String testName, String probeName) {
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

    private static File getLatencyFile(String testId, String probeName) {
        return new File(testId + '-' + probeName + ".hdr");
    }
}
