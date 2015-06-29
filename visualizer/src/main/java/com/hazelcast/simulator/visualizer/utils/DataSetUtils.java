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
package com.hazelcast.simulator.visualizer.utils;

import com.hazelcast.simulator.probes.probes.LinearHistogram;
import com.hazelcast.simulator.probes.probes.Result;
import com.hazelcast.simulator.probes.probes.impl.HdrLatencyDistributionResult;
import com.hazelcast.simulator.probes.probes.impl.LatencyDistributionResult;
import com.hazelcast.simulator.visualizer.data.SimpleHistogramDataSetContainer;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.HistogramIterationValue;
import org.jfree.data.statistics.SimpleHistogramBin;

public final class DataSetUtils {

    private DataSetUtils() {
    }

    public static SimpleHistogramDataSetContainer calculateSingleProbeDataSet(Result probeData, int accuracy,
                                                                              double scalingPercentile) {
        if (probeData instanceof HdrLatencyDistributionResult) {
            return calcSingleProbeDataSet((HdrLatencyDistributionResult) probeData, accuracy, scalingPercentile);
        }
        if (probeData instanceof LatencyDistributionResult) {
            return calcSingleProbeDataSet((LatencyDistributionResult) probeData, accuracy, scalingPercentile);
        }
        return null;
    }

    private static SimpleHistogramDataSetContainer calcSingleProbeDataSet(HdrLatencyDistributionResult probeData, long accuracy,
                                                                          double scalingPercentile) {
        SimpleHistogramDataSetContainer histogramDataSet = new SimpleHistogramDataSetContainer("key");
        histogramDataSet.setAdjustForBinSize(false);

        Histogram histogram = probeData.getHistogram();
        for (HistogramIterationValue value : histogram.linearBucketValues(accuracy)) {
            int values = (int) value.getCountAddedInThisIterationStep();
            if (values > 0) {
                long lowerBound = value.getValueIteratedFrom();
                long upperBound = value.getValueIteratedTo();
                SimpleHistogramBin bin = new SimpleHistogramBin(lowerBound, upperBound, true, false);
                bin.setItemCount(values);
                histogramDataSet.addBin(bin);
            }
        }

        histogramDataSet.setAutoScaleValue(histogram.getValueAtPercentile(scalingPercentile * 100));
        return histogramDataSet;
    }

    private static SimpleHistogramDataSetContainer calcSingleProbeDataSet(LatencyDistributionResult probeData, long accuracy,
                                                                          double scalingPercentile) {
        SimpleHistogramDataSetContainer histogramDataSet = new SimpleHistogramDataSetContainer("key");
        histogramDataSet.setAdjustForBinSize(false);

        LinearHistogram histogram = probeData.getHistogram();
        int histogramStep = histogram.getStep();
        int lowerBound = 0;
        SimpleHistogramBin bin = new SimpleHistogramBin(0, accuracy, true, false);
        for (int values : histogram.getBuckets()) {
            if (lowerBound % accuracy == 0 && lowerBound > 0) {
                addBinIfNotEmpty(histogramDataSet, bin);
                bin = new SimpleHistogramBin(lowerBound, lowerBound + accuracy, true, false);
            }
            if (values > 0) {
                bin.setItemCount(bin.getItemCount() + values);
            }
            lowerBound += histogramStep;
        }
        addBinIfNotEmpty(histogramDataSet, bin);

        histogramDataSet.setAutoScaleValue(histogram.getPercentile(scalingPercentile).getBucket());
        return histogramDataSet;
    }

    private static void addBinIfNotEmpty(SimpleHistogramDataSetContainer histogramDataSet, SimpleHistogramBin bin) {
        if (bin != null && bin.getItemCount() > 0) {
            histogramDataSet.addBin(bin);
        }
    }
}
