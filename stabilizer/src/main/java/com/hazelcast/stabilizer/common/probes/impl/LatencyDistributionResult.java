package com.hazelcast.stabilizer.common.probes.impl;

import com.hazelcast.stabilizer.common.HistogramPart;
import com.hazelcast.stabilizer.common.LinearHistogram;
import com.hazelcast.stabilizer.common.probes.Result;

import java.text.NumberFormat;
import java.util.Locale;

public class LatencyDistributionResult implements Result<LatencyDistributionResult> {
    private final LinearHistogram linearHistogram;

    public LatencyDistributionResult(LinearHistogram linearHistogram) {
        this.linearHistogram = linearHistogram;
    }

    @Override
    public LatencyDistributionResult combine(LatencyDistributionResult other) {
        if (other == null) {
            return this;
        }
        LatencyDistributionResult result = new LatencyDistributionResult(linearHistogram.combine(other.linearHistogram));
        return result;
    }

    @Override
    public String toHumanString() {
        StringBuilder builder = new StringBuilder();
        NumberFormat numberFormat = NumberFormat.getInstance(Locale.US);
        int numberOfSpaces = 15;
        double percentiles[] = {0.999d, 0.99d, 0.9d, 0.85d, 0.8d, 0.75d, 0.7d, 0.6d, 0.5d, 0.4d, 0.3d};
        builder.append('\n');
        for (int i = 0; i < percentiles.length; i++) {
            double percentile = percentiles[i];
            HistogramPart value = linearHistogram.getPercentile(percentile);
            String formattedPercentile = numberFormat.format(percentile);
            builder.append("Percentile ")
                    .append(formattedPercentile).append(" ")
                    .append(String.format("%"+ (numberOfSpaces - formattedPercentile.length()) +"s", value.getBucket()))
                    .append(" µs")
                    .append(String.format("%"+ numberOfSpaces +"s", value.getValues()))
                    .append(" Ops.\n");
        }
        return builder.toString();
    }
}
