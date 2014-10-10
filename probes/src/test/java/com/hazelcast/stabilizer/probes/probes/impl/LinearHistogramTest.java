package com.hazelcast.stabilizer.probes.probes.impl;

import com.hazelcast.stabilizer.probes.probes.LinearHistogram;
import com.hazelcast.stabilizer.probes.probes.impl.HistogramPart;
import com.hazelcast.stabilizer.probes.probes.impl.LatencyDistributionResult;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class LinearHistogramTest {

    private int STEP = 1;

    @Test
    public void testGetPercentile() throws Exception {
        LinearHistogram linearHistogram = createNewHistogram(500);
        HistogramPart percentile = linearHistogram.getPercentile(1.0);
        assertEquals(500, percentile.getValues());
    }

    @Test
    public void foo() {
        LinearHistogram linearHistogram = createNewHistogram(50000);
        LatencyDistributionResult latencyDistributionResult = new LatencyDistributionResult(linearHistogram);
        System.out.println(latencyDistributionResult.toHumanString());
    }

    @Test
    public void testCombine() throws Exception {
        LinearHistogram linearHistogram1 = createNewHistogram(500);
        LinearHistogram linearHistogram2 = createNewHistogram(500);

        LinearHistogram combinedHistogram = linearHistogram1.combine(linearHistogram2);
        HistogramPart percentile = combinedHistogram.getPercentile(1.0);
        assertEquals(1000, percentile.getValues());
        assertEquals(500, percentile.getBucket());
    }

    private LinearHistogram createNewHistogram(int values) {
        LinearHistogram linearHistogram = new LinearHistogram(values, STEP);
        for (int i = 0; i < values; i ++) {
            linearHistogram.addValue(i);
        }
        return linearHistogram;
    }
}