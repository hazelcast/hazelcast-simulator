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
package com.hazelcast.simulator.visualizer.utils;

import com.hazelcast.simulator.visualizer.data.SimulatorHistogramDataSet;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.HistogramIterationValue;
import org.jfree.data.statistics.SimpleHistogramBin;

public final class DataSetUtils {

    private static final int PERCENTILE_FACTOR = 100;

    private DataSetUtils() {
    }

    public static SimulatorHistogramDataSet getHistogramDataSet(Histogram histogram, int accuracy, double scalingPercentile) {
        if (histogram == null) {
            return null;
        }

        SimulatorHistogramDataSet histogramDataSet = new SimulatorHistogramDataSet("key");
        histogramDataSet.setAdjustForBinSize(false);

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

        histogramDataSet.setAutoScaleValue(histogram.getValueAtPercentile(scalingPercentile * PERCENTILE_FACTOR));
        return histogramDataSet;
    }
}
