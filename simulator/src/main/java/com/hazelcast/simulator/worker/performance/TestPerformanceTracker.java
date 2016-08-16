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

import com.hazelcast.simulator.test.TestException;
import com.hazelcast.simulator.testcontainer.TestContainer;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.HistogramLogWriter;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.simulator.utils.FileUtils.getUserDir;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Tracks performance related values for a single Simulator Test.
 *
 * Has methods to update the performance values and write them to files.
 *
 * Holds a map of {@link Histogram} for each {@link com.hazelcast.simulator.probes.Probe} of a Simulator Test.
 */
final class TestPerformanceTracker {

    private static final long ONE_SECOND_IN_MILLIS = SECONDS.toMillis(1);

    private final TestContainer testContainer;
    private final String testId;

    // used to determine if the TestPerformanceTracker can be deleted
    private long lastSeen;

    private final Map<String, HistogramLogWriter> histogramLogWriterMap = new HashMap<String, HistogramLogWriter>();
    private final long testStartedTimestamp;
    private long lastIterations;
    private final PerformanceLogWriter performanceLogWriter;
    private long lastTimestamp;
    private Map<String, Histogram> intervalHistogramMap;
    private double intervalLatencyAvgNanos;
    private long intervalLatency999PercentileNanos;
    private long intervalLatencyMaxNanos;
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
        this.performanceLogWriter = new PerformanceLogWriter(new File(getUserDir(), "performance-" + testId + ".csv"));

        for (String probeName : testContainer.getProbeMap().keySet()) {
            histogramLogWriterMap.put(probeName, createHistogramLogWriter(testId, probeName, testStartedTimestamp));
        }
    }

    public TestContainer getTestContainer() {
        return testContainer;
    }

    String getTestId() {
        return testId;
    }

    long getLastSeen() {
        return lastSeen;
    }

    void setLastSeen(long lastSeen) {
        this.lastSeen = lastSeen;
    }

    long getLastIterations() {
        return lastIterations;
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
                long intervalMaxLatency, long intervalOperationCount, long iterations, long currentTimestamp) {
        this.intervalHistogramMap = intervalHistograms;

        this.intervalLatency999PercentileNanos = intervalPercentileLatency;
        this.intervalLatencyAvgNanos = intervalAvgLatency;
        this.intervalLatencyMaxNanos = intervalMaxLatency;

        this.intervalOperationCount = intervalOperationCount;
        this.totalOperationCount += intervalOperationCount;

        long intervalTimeDelta = currentTimestamp - lastTimestamp;
        long totalTimeDelta = currentTimestamp - testStartedTimestamp;

        this.intervalThroughput = (intervalOperationCount * ONE_SECOND_IN_MILLIS) / (double) intervalTimeDelta;
        this.totalThroughput = (totalOperationCount * ONE_SECOND_IN_MILLIS / (double) totalTimeDelta);

        this.lastIterations = iterations;
        this.lastTimestamp = currentTimestamp;
        this.isUpdated = true;
    }

    void writeStatsToFile(long epochTime, String timestamp) {
        performanceLogWriter.write(
                epochTime,
                timestamp,
                totalOperationCount,
                intervalOperationCount,
                intervalThroughput,
                0,
                0);

        for (Map.Entry<String, Histogram> histogramEntry : intervalHistogramMap.entrySet()) {
            String probeName = histogramEntry.getKey();
            HistogramLogWriter histogramLogWriter = histogramLogWriterMap.get(probeName);

            Histogram intervalHistogram = histogramEntry.getValue();
            histogramLogWriter.outputIntervalHistogram(intervalHistogram);
        }
    }

    PerformanceStats createPerformanceStats() {
        return new PerformanceStats(totalOperationCount, intervalThroughput, totalThroughput,
                intervalLatencyAvgNanos, intervalLatency999PercentileNanos, intervalLatencyMaxNanos);
    }

    static HistogramLogWriter createHistogramLogWriter(String testId, String probeName, long baseTime) {
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

    private static File getLatencyFile(String testId, String probeName) {
        return new File(getUserDir(), testId + '-' + probeName + ".hdr");
    }
}
