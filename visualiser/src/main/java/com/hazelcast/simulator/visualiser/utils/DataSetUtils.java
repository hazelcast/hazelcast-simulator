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

    public static SimpleHistogramDataSetContainer calculateSingleProbeDataSet(Result probeData, int accuracy) {
        if (probeData instanceof LatencyDistributionResult) {
            return calcSingleProbeDataSet((LatencyDistributionResult) probeData, accuracy);
        } else if (probeData instanceof HdrLatencyDistributionResult) {
            return calcSingleProbeDataSet((HdrLatencyDistributionResult) probeData, accuracy);
        }
        throw new IllegalArgumentException("unknown probe result type: " + probeData.getClass().getSimpleName());
    }

    private static SimpleHistogramDataSetContainer calcSingleProbeDataSet(HdrLatencyDistributionResult probeData, long accuracy) {
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
        histogramDataSet.setMaxLatency(histogram.getMaxValue());
        return histogramDataSet;
    }

    private static SimpleHistogramDataSetContainer calcSingleProbeDataSet(LatencyDistributionResult probeData, long accuracy) {
        SimpleHistogramDataSetContainer histogramDataSet = new SimpleHistogramDataSetContainer("key");
        histogramDataSet.setAdjustForBinSize(false);
        LinearHistogram histogram = probeData.getHistogram();
        int histogramStep = histogram.getStep();
        int lowerBound = 0;
        int maxLatency = 0;
        SimpleHistogramBin bin = new SimpleHistogramBin(0, accuracy, true, false);
        for (int values : histogram.getBuckets()) {
            if (lowerBound % accuracy == 0 && lowerBound > 0) {
                addBinIfNotEmpty(histogramDataSet, bin);
                bin = new SimpleHistogramBin(lowerBound, lowerBound + accuracy, true, false);
            }
            if (values > 0) {
                maxLatency = lowerBound;
                bin.setItemCount(bin.getItemCount() + values);
            }
            lowerBound += histogramStep;
        }
        addBinIfNotEmpty(histogramDataSet, bin);
        histogramDataSet.setMaxLatency(maxLatency);
        return histogramDataSet;
    }

    private static void addBinIfNotEmpty(SimpleHistogramDataSetContainer histogramDataSet, SimpleHistogramBin bin) {
        if (bin != null && bin.getItemCount() > 0) {
            histogramDataSet.addBin(bin);
        }
    }
}
