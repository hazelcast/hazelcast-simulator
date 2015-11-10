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
package com.hazelcast.simulator.heatmap;

import com.hazelcast.simulator.utils.CommandLineExitException;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.HistogramLogReader;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import static com.hazelcast.simulator.common.GitInfo.getBuildTime;
import static com.hazelcast.simulator.common.GitInfo.getCommitIdAbbrev;
import static com.hazelcast.simulator.heatmap.HeatMapCli.init;
import static com.hazelcast.simulator.heatmap.HeatMapCli.run;
import static com.hazelcast.simulator.utils.CommonUtils.exitWithError;
import static com.hazelcast.simulator.utils.CommonUtils.getSimulatorVersion;
import static com.hazelcast.simulator.utils.FileUtils.getSimulatorHome;
import static java.lang.String.format;

/**
 * Commandline tool to create heatmaps from Simulator test runs.
 */
public class HeatMap {

    private static final Logger LOGGER = Logger.getLogger(HeatMap.class);

    private final File directory;
    private final String testName;
    private final String probeName;

    private int histogramCount;

    public HeatMap(String directory, String testName, String probeName) {
        LOGGER.info("Hazelcast Simulator HeatMap");
        LOGGER.info(format("Version: %s, Commit: %s, Build Time: %s", getSimulatorVersion(), getCommitIdAbbrev(),
                getBuildTime()));
        LOGGER.info(format("SIMULATOR_HOME: %s", getSimulatorHome()));

        this.directory = new File(directory).getAbsoluteFile();
        this.testName = testName;
        this.probeName = probeName;
    }

    void shutdown() {
    }

    void createHeatMap() {
        LOGGER.info(format("Processing directory %s...", directory));
        HistogramFilenameFilter filenameFilter = new HistogramFilenameFilter(testName, probeName);
        FileWalker fileWalker = new FileWalker(filenameFilter);
        fileWalker.walk(directory);

        ArrayList<Histogram> histograms = new ArrayList<Histogram>();
        for (File latencyFile : fileWalker.getGetFiles()) {
            LOGGER.info(format("Processing latency file %s...", latencyFile.getAbsolutePath()));
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

        histogramCount = histograms.size();
        LOGGER.info(format("Found %d histograms in total", histogramCount));

        long totalMaxLatency = Long.MIN_VALUE;
        for (Histogram histogram : histograms) {
            long maxValue = histogram.getMaxValue();
            LOGGER.info(format("Maximum latency: %d µs", maxValue));
            if (maxValue > totalMaxLatency) {
                totalMaxLatency = maxValue;
            }
        }
        LOGGER.info(format("Total maximum latency: %d µs", totalMaxLatency));
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

    static HistogramLogReader createHistogramLogReader(File latencyFile, String testName) {
        try {
            return new HistogramLogReader(latencyFile);
        } catch (IOException e) {
            throw new CommandLineExitException("Could not initialize HistogramLogReader for test " + testName, e);
        }
    }
}
