package com.hazelcast.simulator.probes.probes.impl;

import org.HdrHistogram.Histogram;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static com.hazelcast.simulator.probes.probes.impl.ProbeTestUtils.TOLERANCE_MILLIS;
import static com.hazelcast.simulator.probes.probes.impl.ProbeTestUtils.assertDisable;
import static com.hazelcast.simulator.probes.probes.impl.ProbeTestUtils.assertResult;
import static com.hazelcast.simulator.probes.probes.impl.ProbeTestUtils.assertWithinTolerance;
import static com.hazelcast.simulator.utils.CommonUtils.sleepNanos;
import static com.hazelcast.simulator.utils.TestUtils.assertEqualsStringFormat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class HdrLatencyDistributionProbeTest {

    private HdrLatencyDistributionProbe hdrLatencyDistributionProbe = new HdrLatencyDistributionProbe();

    @Test
    public void testDisable() {
        assertDisable(hdrLatencyDistributionProbe);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSetValues() {
        hdrLatencyDistributionProbe.setValues(123, 125812);
    }

    @Test(expected = IllegalStateException.class)
    public void testDoneWithoutStarted() {
        hdrLatencyDistributionProbe.done();
    }

    @Test
    public void testInvocationCount() {
        hdrLatencyDistributionProbe.started();
        hdrLatencyDistributionProbe.done();
        hdrLatencyDistributionProbe.done();
        hdrLatencyDistributionProbe.done();
        hdrLatencyDistributionProbe.done();
        hdrLatencyDistributionProbe.done();

        assertEquals(5, hdrLatencyDistributionProbe.getInvocationCount());
    }

    @Test
    public void testStartedDone() {
        int expectedCount = 1;
        long expectedLatency = 150;

        hdrLatencyDistributionProbe.started();
        sleepNanos(TimeUnit.MILLISECONDS.toNanos(expectedLatency));
        hdrLatencyDistributionProbe.done();

        HdrLatencyDistributionResult result = hdrLatencyDistributionProbe.getResult();
        assertResult(result, new HdrLatencyDistributionProbe().getResult());
        assertHistogram(result.getHistogram(), expectedCount, expectedLatency, expectedLatency, expectedLatency);
    }

    @Test
    public void testRecordValues() {
        int expectedCount = 3;
        long latencyValue = 500;
        long expectedMinValue = 200;
        long expectedMaxValue = 1000;
        long expectedMeanValue = (long) ((latencyValue + expectedMinValue + expectedMaxValue) / (double) expectedCount);

        hdrLatencyDistributionProbe.recordValue(TimeUnit.MILLISECONDS.toNanos(latencyValue));
        hdrLatencyDistributionProbe.recordValue(TimeUnit.MILLISECONDS.toNanos(expectedMinValue));
        hdrLatencyDistributionProbe.recordValue(TimeUnit.MILLISECONDS.toNanos(expectedMaxValue));

        HdrLatencyDistributionResult result = hdrLatencyDistributionProbe.getResult();
        assertResult(result, new HdrLatencyDistributionProbe().getResult());
        assertHistogram(result.getHistogram(), expectedCount, expectedMinValue, expectedMaxValue, expectedMeanValue);
    }

    @Test
    public void testResultCombine() {
        int expectedCount = 2;
        long expectedMinValue = 150;
        long expectedMaxValue = 500;
        long expectedMeanValue = (long) ((expectedMinValue + expectedMaxValue) / (double) expectedCount);

        hdrLatencyDistributionProbe.recordValue(TimeUnit.MILLISECONDS.toNanos(expectedMinValue));

        HdrLatencyDistributionResult result1 = hdrLatencyDistributionProbe.getResult();
        assertSingleResult(result1);

        HdrLatencyDistributionProbe hdrLatencyDistributionProbe2 = new HdrLatencyDistributionProbe();
        hdrLatencyDistributionProbe2.recordValue(TimeUnit.MILLISECONDS.toNanos(expectedMaxValue));

        HdrLatencyDistributionResult result2 = hdrLatencyDistributionProbe2.getResult();
        assertSingleResult(result2);

        assertNotEquals(result1.hashCode(), result2.hashCode());

        HdrLatencyDistributionResult combined = result1.combine(result2);
        assertResult(combined, new HdrLatencyDistributionProbe().getResult());
        assertHistogram(combined.getHistogram(), expectedCount, expectedMinValue, expectedMaxValue, expectedMeanValue);
    }

    private static void assertSingleResult(HdrLatencyDistributionResult result) {
        assertTrue(result != null);
        assertEqualsStringFormat("Expected %d records, but was %d", 1L, result.getHistogram().getTotalCount());
    }

    private static void assertHistogram(Histogram histogram, long expectedCount, long expectedMinValueMillis,
                                        long expectedMaxValueMillis, long expectedMeanValueMillis) {
        long toleranceMicros = TimeUnit.MILLISECONDS.toMicros(TOLERANCE_MILLIS);

        long minValue = histogram.getMinValue();
        long maxValue = histogram.getMaxValue();
        assertNotEquals("Expected minValue and maxValue to differ", minValue, maxValue);
        assertWithinTolerance("minValue", TimeUnit.MILLISECONDS.toMicros(expectedMinValueMillis), minValue, toleranceMicros);
        assertWithinTolerance("maxValue", TimeUnit.MILLISECONDS.toMicros(expectedMaxValueMillis), maxValue, toleranceMicros);

        long meanValue = (long) histogram.getMean();
        assertWithinTolerance("meanValue", TimeUnit.MILLISECONDS.toMicros(expectedMeanValueMillis), meanValue, toleranceMicros);

        assertEqualsStringFormat("Expected %d records, but was %d", expectedCount, histogram.getTotalCount());
    }
}
