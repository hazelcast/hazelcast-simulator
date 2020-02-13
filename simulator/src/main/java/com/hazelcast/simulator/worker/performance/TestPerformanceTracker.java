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
import com.hazelcast.simulator.probes.impl.HdrProbe;
import com.hazelcast.simulator.test.TestException;
import com.hazelcast.simulator.worker.testcontainer.TestContainer;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.HistogramLogWriter;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.simulator.utils.FileUtils.getUserDir;
import static com.hazelcast.simulator.worker.performance.PerformanceStats.INTERVAL_LATENCY_PERCENTILE;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Tracks performance related values for a single Simulator Test.
 * <p>
 * Has methods to update the performance values and write them to files.
 * <p>
 * Holds a map of {@link Histogram} for each {@link com.hazelcast.simulator.probes.Probe} of a Simulator Test.
 *
 * This class has a shitty design due to conflated concerns due to file writing and remoting sending the histograms.
 * This is caused by the Recorder that gets reset when getIntervalHistogram is called. Meaning that writing to file
 * and sending to remote, needs to rely on the same set of Histograms to write/send.
 */
public final class TestPerformanceTracker {

    private static final long ONE_SECOND_IN_MILLIS = SECONDS.toMillis(1);

    private final TestContainer testContainer;
    private final Map<String, HistogramLogWriter> histogramLogWriterMap = new HashMap<>();
    private final PerformanceLogWriter performanceLogWriter;
    private long lastUpdateMillis;
    private Map<String, Histogram> intervalHistogramMap;

    private long iterationsDuringWarmup;
    private long lastIterations;
    private double intervalLatencyAvgNanos;
    private long intervalLatency999PercentileNanos;
    private long intervalLatencyMaxNanos;
    private long intervalOperationCount;
    private long totalOperationCount;
    private double intervalThroughput;
    private double totalThroughput;
    private long nextUpdateMillis;

    public TestPerformanceTracker(TestContainer container) {
        this.testContainer = container;
        this.performanceLogWriter = new PerformanceLogWriter(
                new File(getUserDir(), "performance-" + container.getTestCase().getId() + ".csv"));
    }

    /**
     * Updates internal state.
     *
     * @param updateIntervalMillis update interval in millis
     * @param currentTimeMillis current time in millis
     * @return true if anything needs to be written; false otherwise
     */
    public boolean update(long updateIntervalMillis, long currentTimeMillis) {
        if (skipUpdate(updateIntervalMillis, currentTimeMillis)) {
            return false;
        }

        makeUpdate(updateIntervalMillis, currentTimeMillis);
        return true;
    }

    private boolean skipUpdate(long updateIntervalMillis, long currentTimeMillis) {
        long runStartedMillis = testContainer.getRunStartedMillis();

        if (!testContainer.isRunning() || runStartedMillis == 0) {
            // the test hasn't started
            return true;
        }

        if (lastUpdateMillis == 0) {
            // first time
            iterationsDuringWarmup = testContainer.iteration();
            for (Probe probe : testContainer.getProbeMap().values()) {
                probe.reset();
            }
            lastUpdateMillis = currentTimeMillis;
            nextUpdateMillis = lastUpdateMillis + updateIntervalMillis;
            return true;
        }

        return nextUpdateMillis > currentTimeMillis;
    }

    private void makeUpdate(long updateIntervalMillis, long currentTimeMillis) {
        Map<String, Probe> probeMap = testContainer.getProbeMap();
        Map<String, Histogram> intervalHistograms = new HashMap<>(probeMap.size());

        long intervalPercentileLatency = -1;
        double intervalMean = -1;
        long intervalMaxLatency = -1;

        long iterations = testContainer.iteration() - iterationsDuringWarmup;
        long intervalOperationCount = iterations - lastIterations;

        for (Map.Entry<String, Probe> entry : probeMap.entrySet()) {
            String probeName = entry.getKey();
            Probe probe = entry.getValue();
            if (!(probe instanceof HdrProbe)) {
                continue;
            }

            HdrProbe hdrProbe = (HdrProbe) probe;
            Histogram intervalHistogram = hdrProbe.getRecorder().getIntervalHistogram();
            intervalHistogram.setStartTimeStamp(lastUpdateMillis);
            intervalHistogram.setEndTimeStamp(currentTimeMillis);
            intervalHistograms.put(probeName, intervalHistogram);

            long percentileValue = intervalHistogram.getValueAtPercentile(INTERVAL_LATENCY_PERCENTILE);
            if (percentileValue > intervalPercentileLatency) {
                intervalPercentileLatency = percentileValue;
            }

            double meanLatency = intervalHistogram.getMean();
            if (meanLatency > intervalMean) {
                intervalMean = meanLatency;
            }

            long maxValue = intervalHistogram.getMaxValue();
            if (maxValue > intervalMaxLatency) {
                intervalMaxLatency = maxValue;
            }

            if (probe.isPartOfTotalThroughput()) {
                intervalOperationCount += intervalHistogram.getTotalCount();
            }
        }

        this.intervalHistogramMap = intervalHistograms;

        this.intervalLatency999PercentileNanos = intervalPercentileLatency;
        this.intervalLatencyAvgNanos = intervalMean;
        this.intervalLatencyMaxNanos = intervalMaxLatency;

        this.intervalOperationCount = intervalOperationCount;
        this.totalOperationCount += intervalOperationCount;

        long intervalTimeDelta = currentTimeMillis - lastUpdateMillis;
        long totalTimeDelta = currentTimeMillis - testContainer.getRunStartedMillis();

        this.intervalThroughput = (intervalOperationCount * ONE_SECOND_IN_MILLIS) / (double) intervalTimeDelta;
        this.totalThroughput = (totalOperationCount * ONE_SECOND_IN_MILLIS / (double) totalTimeDelta);

        this.lastIterations = iterations;
        this.nextUpdateMillis += updateIntervalMillis;
        this.lastUpdateMillis = currentTimeMillis;
    }

    long intervalOperationCount() {
        return intervalOperationCount;
    }

    long totalOperationCount() {
        return totalOperationCount;
    }

    double intervalThroughput() {
        return intervalThroughput;
    }

    void persist(long currentTimeMillis, String currentTimeString) {
        performanceLogWriter.write(
                currentTimeMillis,
                currentTimeString,
                totalOperationCount,
                intervalOperationCount,
                intervalThroughput);

        // dumps all the Histograms that have been collected to file.
        for (Map.Entry<String, Histogram> histogramEntry : intervalHistogramMap.entrySet()) {
            String probeName = histogramEntry.getKey();
            HistogramLogWriter histogramLogWriter = histogramLogWriterMap.get(probeName);
            if (histogramLogWriter == null) {
                histogramLogWriter = createHistogramLogWriter(probeName);
                histogramLogWriterMap.put(probeName, histogramLogWriter);
            }
            Histogram intervalHistogram = histogramEntry.getValue();
            histogramLogWriter.outputIntervalHistogram(intervalHistogram);
        }
    }

    PerformanceStats createPerformanceStats() {
        return new PerformanceStats(
                totalOperationCount,
                intervalThroughput,
                totalThroughput,
                intervalLatencyAvgNanos,
                intervalLatency999PercentileNanos,
                intervalLatencyMaxNanos);
    }

    HistogramLogWriter createHistogramLogWriter(String probeName) {
        String testId = testContainer.getTestCase().getId();
        try {
            File latencyFile = getLatencyFile(testId, probeName);
            HistogramLogWriter histogramLogWriter = new HistogramLogWriter(latencyFile);
            histogramLogWriter.setBaseTime(testContainer.getRunStartedMillis());
            histogramLogWriter.outputStartTime(testContainer.getRunStartedMillis());
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
