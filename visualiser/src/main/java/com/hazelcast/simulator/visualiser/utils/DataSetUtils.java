package com.hazelcast.simulator.visualiser.utils;

import com.hazelcast.simulator.probes.probes.LinearHistogram;
import com.hazelcast.simulator.probes.probes.Result;
import com.hazelcast.simulator.probes.probes.impl.HdrLatencyDistributionResult;
import com.hazelcast.simulator.probes.probes.impl.LatencyDistributionResult;
import com.hazelcast.simulator.visualiser.data.SimpleHistogramDataSetContainer;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.HistogramIterationValue;
import org.jfree.data.statistics.SimpleHistogramBin;

public final class DataSetUtils {

    private DataSetUtils() {
    }

    public static SimpleHistogramDataSetContainer calculateSingleProbeDataSet(Result probeData) {
        if (probeData instanceof LatencyDistributionResult) {
            return calculateSingleProbeDataSet((LatencyDistributionResult) probeData);
        } else if (probeData instanceof HdrLatencyDistributionResult) {
            return calculateSingleProbeDataSet((HdrLatencyDistributionResult) probeData);
        }
        throw new IllegalArgumentException("unknown probe result type: " + probeData.getClass().getSimpleName());
    }

    private static SimpleHistogramDataSetContainer calculateSingleProbeDataSet(HdrLatencyDistributionResult probeData) {
        SimpleHistogramDataSetContainer histogramDataSet = new SimpleHistogramDataSetContainer("key");
        histogramDataSet.setAdjustForBinSize(false);
        Histogram histogram = probeData.getHistogram();
        for (HistogramIterationValue value : histogram.linearBucketValues(10)) {
            double lowerBound = value.getDoubleValueIteratedFrom();
            double upperBound = value.getDoubleValueIteratedTo();
            SimpleHistogramBin bin = new SimpleHistogramBin(lowerBound, upperBound, true, false);
            bin.setItemCount((int) value.getCountAddedInThisIterationStep());
            histogramDataSet.addBin(bin);
        }
        histogramDataSet.setMaxLatency(histogram.getMaxValue());
        return histogramDataSet;
    }

    private static SimpleHistogramDataSetContainer calculateSingleProbeDataSet(LatencyDistributionResult probeData) {
        SimpleHistogramDataSetContainer histogramDataSet = new SimpleHistogramDataSetContainer("key");
        LinearHistogram histogram = probeData.getHistogram();
        int step = histogram.getStep();
        int lowerBound = 0;
        int maxLatency = 0;
        for (int values : histogram.getBuckets()) {
            int upperBound = lowerBound + step;
            if (values > 0) {
                maxLatency = upperBound;
            }
            SimpleHistogramBin bin = new SimpleHistogramBin(lowerBound, upperBound, true, false);
            bin.setItemCount(values);
            histogramDataSet.addBin(bin);
            lowerBound += step;
        }
        histogramDataSet.setMaxLatency(maxLatency);
        return histogramDataSet;
    }
}
