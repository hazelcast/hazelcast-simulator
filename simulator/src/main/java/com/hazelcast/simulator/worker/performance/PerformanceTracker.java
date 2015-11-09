/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

import static com.hazelcast.simulator.probes.impl.ProbeImpl.LATENCY_PRECISION;
import static com.hazelcast.simulator.probes.impl.ProbeImpl.MAXIMUM_LATENCY;
import static com.hazelcast.simulator.worker.performance.PerformanceUtils.ONE_SECOND_IN_MILLIS;
import static com.hazelcast.simulator.worker.performance.PerformanceUtils.writeThroughputHeader;
import static com.hazelcast.simulator.worker.performance.PerformanceUtils.writeThroughputStats;

final class PerformanceTracker {

    private final File throughputFile;
    private final Map<String, HistogramLogWriter> histogramLogWriterMap = new HashMap<String, HistogramLogWriter>();
    private final long testStartedTimestamp;

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

    PerformanceTracker(String testId, Collection<String> probeNames, long testStartedTimestamp) {
        throughputFile = new File("throughput-" + testId + ".txt");
        writeThroughputHeader(throughputFile, false);

        for (String probeName : probeNames) {
            histogramLogWriterMap.put(probeName, createHistogramLogWriter(testId, probeName, testStartedTimestamp));
        }

        this.testStartedTimestamp = testStartedTimestamp;
        this.lastTimestamp = testStartedTimestamp;
    }

    long getIntervalOperationCount() {
        return intervalOperationCount;
    }

    public long getTotalOperationCount() {
        return totalOperationCount;
    }

    double getIntervalThroughput() {
        return intervalThroughput;
    }

    public boolean isUpdated() {
        return isUpdated;
    }

    public boolean getAndResetIsUpdated() {
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

    void writeStatsToFile(String timestamp) {
        writeThroughputStats(throughputFile, timestamp, totalOperationCount, intervalOperationCount, intervalThroughput, 0, 0);

        for (Map.Entry<String, Histogram> histogramEntry : intervalHistogramMap.entrySet()) {
            String probeName = histogramEntry.getKey();
            HistogramLogWriter histogramLogWriter = histogramLogWriterMap.get(probeName);

            Histogram intervalHistogram = histogramEntry.getValue();
            histogramLogWriter.outputIntervalHistogram(intervalHistogram);
        }
    }

    PerformanceState createPerformanceState() {
        return new PerformanceState(totalOperationCount, intervalThroughput, totalThroughput,
                intervalAvgLatency, intervalPercentileLatency, intervalMaxLatency);
    }

    Map<String, String> aggregateIntervalHistograms(String testId) {
        Map<String, String> probeResults = new HashMap<String, String>();

        HistogramLogWriter histogramLogWriter = createHistogramLogWriter(testId, "aggregated", 0);
        for (Map.Entry<String, Histogram> histogramEntry : intervalHistogramMap.entrySet()) {
            String probeName = histogramEntry.getKey();
            HistogramLogReader histogramLogReader = createHistogramLogReader(testId, probeName);
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

    private static HistogramLogWriter createHistogramLogWriter(String testId, String probeName, long baseTime) {
        try {
            File latencyFile = getLatencyFile(testId, probeName);
            HistogramLogWriter histogramLogWriter = new HistogramLogWriter(latencyFile);
            histogramLogWriter.setBaseTime(baseTime);
            histogramLogWriter.outputComment("[Latency histograms for " + testId + '.' + probeName + ']');
            histogramLogWriter.outputLogFormatVersion();
            histogramLogWriter.outputLegend();
            return histogramLogWriter;
        } catch (IOException e) {
            throw new TestException("Could not initialize HistogramLogWriter for test " + testId, e);
        }
    }

    private static HistogramLogReader createHistogramLogReader(String testName, String probeName) {
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
        return new File("latency-" + testId + '-' + probeName + ".txt");
    }
}
