package com.hazelcast.simulator.probes.probes;

import com.hazelcast.simulator.probes.probes.impl.HistogramPart;
import com.hazelcast.simulator.probes.probes.impl.LatencyDistributionResult;
import org.apache.log4j.Logger;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class LinearHistogramTest {

    private static final Logger LOGGER = Logger.getLogger(LinearHistogramTest.class);

    @Test(expected = IllegalArgumentException.class)
    public void constructor_IllegalArgument_maxValue() {
        new LinearHistogram(0, 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void constructor_IllegalArgument_step() {
        new LinearHistogram(1, 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void addValue_IllegalArgument() {
        LinearHistogram linearHistogram = createNewHistogram(500);
        linearHistogram.addValue(-1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void addMultipleValues_illegalArgument() {
        LinearHistogram linearHistogram = createNewHistogram(5);
        linearHistogram.addMultipleValues(-1, 0);
    }

    @Test
    public void addMultipleValues_zeroTimes() {
        LinearHistogram linearHistogram = createNewHistogram(5);
        linearHistogram.addMultipleValues(1, 0);
        HistogramPart percentile = linearHistogram.getPercentile(1.0);
        assertEquals(5, percentile.getValues());
    }

    @Test
    public void addMultipleValues() {
        LinearHistogram linearHistogram = createNewHistogram(5);
        linearHistogram.addMultipleValues(1, 5);
        HistogramPart percentile = linearHistogram.getPercentile(1.0);
        assertEquals(10, percentile.getValues());
    }

    @Test
    public void getMaxValue() {
        LinearHistogram linearHistogram = createNewHistogram(23);
        assertEquals(23, linearHistogram.getMaxValue());
    }

    @Test
    public void getStep() {
        LinearHistogram linearHistogram = createNewHistogram(23);
        assertEquals(1, linearHistogram.getStep());
    }

    @Test
    @SuppressWarnings("all")
    public void equals_self() {
        LinearHistogram linearHistogram = createNewHistogram(23);
        assertTrue(linearHistogram.equals(linearHistogram));
    }

    @Test
    @SuppressWarnings("all")
    public void equals_null() {
        LinearHistogram linearHistogram = new LinearHistogram(23, 1);
        assertFalse(linearHistogram.equals(null));
    }

    @Test
    public void equals_sameParameter() {
        LinearHistogram linearHistogram1 = new LinearHistogram(23, 1);
        LinearHistogram linearHistogram2 = new LinearHistogram(23, 1);
        assertTrue(linearHistogram1.equals(linearHistogram2));
    }

    @Test
    public void equals_maxValue() {
        LinearHistogram linearHistogram1 = new LinearHistogram(23, 1);
        LinearHistogram linearHistogram2 = new LinearHistogram(42, 1);
        assertFalse(linearHistogram1.equals(linearHistogram2));
    }

    @Test
    public void equals_step() {
        LinearHistogram linearHistogram1 = new LinearHistogram(23, 1);
        LinearHistogram linearHistogram2 = new LinearHistogram(23, 10);
        assertFalse(linearHistogram1.equals(linearHistogram2));
    }

    @Test
    public void equals_buckets() {
        LinearHistogram linearHistogram1 = new LinearHistogram(23, 1);
        LinearHistogram linearHistogram2 = new LinearHistogram(23, 1);
        linearHistogram2.addMultipleValues(1, 5);
        assertFalse(linearHistogram1.equals(linearHistogram2));
    }

    @Test
    public void addValue_overMaxValue() {
        LinearHistogram linearHistogram = createNewHistogram(5);
        linearHistogram.addValue(10);
        HistogramPart percentile = linearHistogram.getPercentile(1.0);
        assertEquals(6, percentile.getValues());
    }

    @Test
    public void printLatencyDistributionResult() {
        LinearHistogram linearHistogram = createNewHistogram(50000);
        LatencyDistributionResult latencyDistributionResult = new LatencyDistributionResult(linearHistogram);
        LOGGER.info(latencyDistributionResult.toHumanString());
    }

    @Test
    public void getPercentile() throws Exception {
        LinearHistogram linearHistogram = createNewHistogram(500);
        HistogramPart percentile = linearHistogram.getPercentile(1.0);
        assertEquals(500, percentile.getValues());
    }

    @Test
    public void combine() throws Exception {
        LinearHistogram linearHistogram1 = createNewHistogram(500);
        LinearHistogram linearHistogram2 = createNewHistogram(500);

        LinearHistogram combinedHistogram = linearHistogram1.combine(linearHistogram2);
        HistogramPart percentile = combinedHistogram.getPercentile(1.0);
        assertEquals(1000, percentile.getValues());
        assertEquals(500, percentile.getBucket());
    }

    @Test(expected = IllegalStateException.class)
    public void combine_differentMaxValue() throws Exception {
        LinearHistogram linearHistogram1 = createNewHistogram(5);
        LinearHistogram linearHistogram2 = createNewHistogram(10);

        linearHistogram1.combine(linearHistogram2);
    }

    @Test(expected = IllegalStateException.class)
    public void combine_differentStep() throws Exception {
        LinearHistogram linearHistogram1 = new LinearHistogram(23, 1);
        LinearHistogram linearHistogram2 = new LinearHistogram(23, 10);

        linearHistogram1.combine(linearHistogram2);
    }

    private static LinearHistogram createNewHistogram(int values) {
        LinearHistogram linearHistogram = new LinearHistogram(values, 1);
        for (int i = 0; i < values; i++) {
            linearHistogram.addValue(i);
        }
        return linearHistogram;
    }
}