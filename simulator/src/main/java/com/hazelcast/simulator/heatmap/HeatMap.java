/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.simulator.heatmap;

import com.hazelcast.simulator.utils.CommandLineExitException;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.HistogramLogReader;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.simulator.common.GitInfo.getBuildTime;
import static com.hazelcast.simulator.common.GitInfo.getCommitIdAbbrev;
import static com.hazelcast.simulator.heatmap.HeatMapCli.init;
import static com.hazelcast.simulator.heatmap.HeatMapCli.run;
import static com.hazelcast.simulator.utils.CommonUtils.exitWithError;
import static com.hazelcast.simulator.utils.CommonUtils.getSimulatorVersion;
import static com.hazelcast.simulator.utils.FileUtils.getSimulatorHome;
import static java.lang.Math.round;
import static java.lang.String.format;

/**
 * Commandline tool to create heatmaps from Simulator test runs.
 */
public class HeatMap {

    private static final int DIMENSION_Y = 800;

    private static final Logger LOGGER = Logger.getLogger(HeatMap.class);

    private final File directory;
    private final String testName;
    private final String probeName;

    private int histogramCount;

    HeatMap(String directory, String testName, String probeName) {
        this.directory = new File(directory).getAbsoluteFile();
        this.testName = testName;
        this.probeName = probeName;
    }

    void createHeatMap() {
        echo("Processing directory %s...", directory);
        HistogramFilenameFilter filenameFilter = new HistogramFilenameFilter(testName, probeName);
        FileWalker fileWalker = new FileWalker(filenameFilter);
        fileWalker.walk(directory);

        List<Histogram> histograms = getHistograms(testName, fileWalker);
        histogramCount = histograms.size();
        echo("Found %d histograms in total", histogramCount);

        long totalMinLatency = Long.MAX_VALUE;
        long totalMaxLatency = Long.MIN_VALUE;
        for (Histogram histogram : histograms) {
            long minValue = histogram.getMinValue();
            if (minValue < totalMinLatency) {
                totalMinLatency = minValue;
            }
            long maxValue = histogram.getMaxValue();
            if (maxValue > totalMaxLatency) {
                totalMaxLatency = maxValue;
            }
            echo("Minimum latency: %d µs, maximum latency: %d µs", minValue, maxValue);
        }
        echo("Total minimum latency: %d µs, total maximum latency: %d µs", totalMinLatency, totalMaxLatency);

        double latencyWindowSize = totalMaxLatency / (double) DIMENSION_Y;
        echo("Latency window per pixel: %.2f µs", latencyWindowSize);

        calculateLinearHeatMap(histograms, latencyWindowSize, totalMaxLatency);
    }

    // just for testing
    int getHistogramCount() {
        return histogramCount;
    }

    public static void main(String[] args) {
        try {
            run(init(args));
        } catch (Exception e) {
            exitWithError(LOGGER, "Could not create heatmap!", e);
        }
    }

    static void logHeader() {
        echo("Hazelcast Simulator HeatMap");
        echo("Version: %s, Commit: %s, Build Time: %s", getSimulatorVersion(), getCommitIdAbbrev(), getBuildTime());
        echo("SIMULATOR_HOME: %s", getSimulatorHome().getAbsolutePath());
    }

    static HistogramLogReader createHistogramLogReader(File latencyFile, String testName) {
        try {
            return new HistogramLogReader(latencyFile);
        } catch (IOException e) {
            throw new CommandLineExitException("Could not initialize HistogramLogReader for test " + testName, e);
        }
    }

    private static List<Histogram> getHistograms(String testName, FileWalker fileWalker) {
        ArrayList<Histogram> histograms = new ArrayList<Histogram>();
        for (File latencyFile : fileWalker.getGetFiles()) {
            echo("Processing latency file %s...", latencyFile.getAbsolutePath());
            HistogramLogReader histogramLogReader = createHistogramLogReader(latencyFile, testName);

            int index = 0;
            Histogram histogram = (Histogram) histogramLogReader.nextIntervalHistogram();
            while (histogram != null) {
                if (histograms.size() > index) {
                    Histogram combined = histograms.get(index);
                    combined.add(histogram);
                } else {
                    histograms.add(histogram);
                }
                index++;
                histogram = (Histogram) histogramLogReader.nextIntervalHistogram();
            }
        }
        return histograms;
    }

    private static List<ArrayList<Long>> calculateLinearHeatMap(List<Histogram> histograms, double latencyWindowSize,
                                                                long totalMaxLatency) {
        int histogramCount = histograms.size();

        ArrayList<ArrayList<Long>> heatmap = new ArrayList<ArrayList<Long>>(histogramCount);
        for (int i = 0; i < histogramCount; i++) {
            heatmap.add(new ArrayList<Long>(DIMENSION_Y));
        }

        for (int yPos = 1; yPos <= DIMENSION_Y; yPos++) {
            long lowValue = (yPos == 1) ? 0 : round((yPos - 1) * latencyWindowSize);
            long highValue = (yPos == DIMENSION_Y) ? totalMaxLatency : round(yPos * latencyWindowSize);
            long maxValue = Long.MIN_VALUE;
            for (int histogramIndex = 0; histogramIndex < histogramCount; histogramIndex++) {
                Histogram histogram = histograms.get(histogramIndex);
                long latencyCount = histogram.getCountBetweenValues(lowValue, highValue);
                if (latencyCount > maxValue) {
                    maxValue = latencyCount;
                }
                heatmap.get(histogramIndex).add(latencyCount);
            }
            echo("%4d: lowValue: %d, highValue: %d, maxValue: %d", yPos, lowValue, highValue, maxValue);
        }
        return heatmap;
    }

    private static void echo(String message, Object... args) {
        LOGGER.info(message == null ? "null" : format(message, args));
    }
}
