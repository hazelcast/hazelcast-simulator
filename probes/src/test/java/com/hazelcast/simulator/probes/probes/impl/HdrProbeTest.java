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

public class HdrProbeTest {

    private HdrProbe hdrProbe = new HdrProbe();

    @Test
    public void testDisable() {
        assertDisable(hdrProbe);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSetValues() {
        hdrProbe.setValues(123, 125812);
    }

    @Test(expected = IllegalStateException.class)
    public void testDoneWithoutStarted() {
        hdrProbe.done();
    }

    @Test
    public void testInvocationCount() {
        hdrProbe.started();
        hdrProbe.done();
        hdrProbe.done();
        hdrProbe.done();
        hdrProbe.done();
        hdrProbe.done();

        assertEquals(5, hdrProbe.getInvocationCount());
    }

    @Test
    public void testStartedDone() {
        int expectedCount = 1;
        long expectedLatency = 150;

        hdrProbe.started();
        sleepNanos(TimeUnit.MILLISECONDS.toNanos(expectedLatency));
        hdrProbe.done();

        HdrResult result = hdrProbe.getResult();
        assertResult(result, new HdrProbe().getResult());
        assertHistogram(result.getHistogram(), expectedCount, expectedLatency, expectedLatency, expectedLatency);
    }

    @Test
    public void testRecordValues() {
        int expectedCount = 3;
        long latencyValue = 500;
        long expectedMinValue = 200;
        long expectedMaxValue = 1000;
        long expectedMeanValue = (long) ((latencyValue + expectedMinValue + expectedMaxValue) / (double) expectedCount);

        hdrProbe.recordValue(TimeUnit.MILLISECONDS.toNanos(latencyValue));
        hdrProbe.recordValue(TimeUnit.MILLISECONDS.toNanos(expectedMinValue));
        hdrProbe.recordValue(TimeUnit.MILLISECONDS.toNanos(expectedMaxValue));

        HdrResult result = hdrProbe.getResult();
        assertResult(result, new HdrProbe().getResult());
        assertHistogram(result.getHistogram(), expectedCount, expectedMinValue, expectedMaxValue, expectedMeanValue);
    }

    @Test
    public void testResultCombine() {
        int expectedCount = 2;
        long expectedMinValue = 150;
        long expectedMaxValue = 500;
        long expectedMeanValue = (long) ((expectedMinValue + expectedMaxValue) / (double) expectedCount);

        hdrProbe.recordValue(TimeUnit.MILLISECONDS.toNanos(expectedMinValue));

        HdrResult result1 = hdrProbe.getResult();
        assertSingleResult(result1);

        HdrProbe hdrProbe2 = new HdrProbe();
        hdrProbe2.recordValue(TimeUnit.MILLISECONDS.toNanos(expectedMaxValue));

        HdrResult result2 = hdrProbe2.getResult();
        assertSingleResult(result2);

        assertNotEquals(result1.hashCode(), result2.hashCode());

        HdrResult combined = result1.combine(result2);
        assertResult(combined, new HdrProbe().getResult());
        assertHistogram(combined.getHistogram(), expectedCount, expectedMinValue, expectedMaxValue, expectedMeanValue);
    }

    private static void assertSingleResult(HdrResult result) {
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
