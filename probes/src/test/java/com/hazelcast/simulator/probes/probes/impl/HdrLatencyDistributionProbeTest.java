package com.hazelcast.simulator.probes.probes.impl;

import org.HdrHistogram.Histogram;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static com.hazelcast.simulator.utils.CommonUtils.sleepNanos;
import static com.hazelcast.simulator.utils.TestUtils.assertEqualsStringFormat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class HdrLatencyDistributionProbeTest {

    private HdrLatencyDistributionProbe hdrLatencyDistributionProbe = new HdrLatencyDistributionProbe();

    @Test(expected = IllegalStateException.class)
    public void testDoneWithoutStarted() {
        hdrLatencyDistributionProbe.done();
    }

    @Test
    public void testInvocations() {
        hdrLatencyDistributionProbe.started();
        hdrLatencyDistributionProbe.done();
        hdrLatencyDistributionProbe.done();
        hdrLatencyDistributionProbe.done();
        hdrLatencyDistributionProbe.done();
        hdrLatencyDistributionProbe.done();

        assertEquals(5, hdrLatencyDistributionProbe.getInvocationCount());
    }

    @Test
    public void testRecordValues() {
        hdrLatencyDistributionProbe.recordValue(TimeUnit.MILLISECONDS.toNanos(500));
        hdrLatencyDistributionProbe.recordValue(TimeUnit.MILLISECONDS.toNanos(200));
        hdrLatencyDistributionProbe.recordValue(TimeUnit.MILLISECONDS.toNanos(1000));

        assertEquals(3, hdrLatencyDistributionProbe.getInvocationCount());
    }

    @Test
    public void testResult() {
        long sleepTime = TimeUnit.MILLISECONDS.toMicros(150);
        long tolerance = TimeUnit.MILLISECONDS.toMicros(5);

        hdrLatencyDistributionProbe.started();
        sleepNanos(TimeUnit.MICROSECONDS.toNanos(sleepTime));
        hdrLatencyDistributionProbe.done();

        HdrLatencyDistributionResult result = hdrLatencyDistributionProbe.getResult();
        assertTrue(result != null);

        Histogram histogram = result.getHistogram();
        assertEqualsStringFormat("Expected %d records, but was %d", 1L, histogram.getTotalCount());

        long minValue = histogram.getMinValue();
        long maxValue = histogram.getMaxValue();
        long diff = maxValue - minValue;
        assertTrue("Expected minValue and maxValue within a range of " + tolerance + " µs, but was " + diff, diff < tolerance);

        double latency = histogram.getMean();
        assertTrue("latency should be >= " + sleepTime + ", but was " + latency, latency >= sleepTime);
        assertTrue("latency should be <= " + (sleepTime + tolerance) + ", but was " + latency, latency <= sleepTime + tolerance);
    }

    @Test
    public void testResult_withRecordValues() {
        long sleepTime = TimeUnit.MILLISECONDS.toMicros(200);
        long tolerance = TimeUnit.MILLISECONDS.toMicros(801);

        hdrLatencyDistributionProbe.recordValue(TimeUnit.MILLISECONDS.toNanos(500));
        hdrLatencyDistributionProbe.recordValue(TimeUnit.MILLISECONDS.toNanos(200));
        hdrLatencyDistributionProbe.recordValue(TimeUnit.MILLISECONDS.toNanos(1000));

        HdrLatencyDistributionResult result = hdrLatencyDistributionProbe.getResult();
        assertTrue(result != null);

        Histogram histogram = result.getHistogram();
        assertEqualsStringFormat("Expected %d records, but was %d", 3L, histogram.getTotalCount());

        long minValue = histogram.getMinValue();
        long maxValue = histogram.getMaxValue();
        long diff = maxValue - minValue;
        assertTrue("Expected minValue and maxValue within a range of " + tolerance + " µs, but was " + diff, diff < tolerance);

        double latency = histogram.getMean();
        assertTrue("latency should be >= " + sleepTime + ", but was " + latency, latency >= sleepTime);
        assertTrue("latency should be <= " + (sleepTime + tolerance) + ", but was " + latency, latency <= sleepTime + tolerance);
    }

    @Test
    public void testResultToHumanString() {
        hdrLatencyDistributionProbe.started();
        sleepNanos(TimeUnit.MICROSECONDS.toNanos(150));
        hdrLatencyDistributionProbe.done();

        HdrLatencyDistributionResult result = hdrLatencyDistributionProbe.getResult();
        assertTrue(result != null);
        assertTrue(result.toHumanString() != null);
    }

    @Test
    public void testResultCombine() {
        long sleepTime1 = TimeUnit.MILLISECONDS.toMicros(150);
        long sleepTime2 = TimeUnit.MILLISECONDS.toMicros(500);
        long tolerance = TimeUnit.MILLISECONDS.toMicros(5);

        hdrLatencyDistributionProbe.started();
        sleepNanos(TimeUnit.MICROSECONDS.toNanos(sleepTime1));
        hdrLatencyDistributionProbe.done();

        HdrLatencyDistributionResult result1 = hdrLatencyDistributionProbe.getResult();
        assertTrue(result1 != null);
        assertEqualsStringFormat("Expected %d records, but was %d", 1L, result1.getHistogram().getTotalCount());

        HdrLatencyDistributionProbe hdrLatencyDistributionProbe2 = new HdrLatencyDistributionProbe();
        hdrLatencyDistributionProbe2.started();
        sleepNanos(TimeUnit.MICROSECONDS.toNanos(sleepTime2));
        hdrLatencyDistributionProbe2.done();

        HdrLatencyDistributionResult result2 = hdrLatencyDistributionProbe2.getResult();
        assertTrue(result2 != null);
        assertEqualsStringFormat("Expected %d records, but was %d", 1L, result2.getHistogram().getTotalCount());

        HdrLatencyDistributionResult combined = result1.combine(result2);
        assertTrue(combined != null);

        Histogram histogram = combined.getHistogram();
        assertEqualsStringFormat("Expected %d records, but was %d", 2L, histogram.getTotalCount());

        long minValue = histogram.getMinValue();
        long maxValue = histogram.getMaxValue();
        assertNotEquals("Expected minValue and maxValue to differ", minValue, maxValue);
        assertTrue("Expected minValue >= " + (sleepTime1 - tolerance) + ", but was " + minValue,
                minValue >= sleepTime1 - tolerance);
        assertTrue("Expected minValue <= " + (sleepTime1 + tolerance) + ", but was " + minValue,
                minValue <= sleepTime1 + tolerance);
        assertTrue("Expected maxValue >= " + (sleepTime2 - tolerance) + ", but was " + maxValue,
                maxValue >= sleepTime2 - tolerance);
        assertTrue("Expected maxValue <= " + (sleepTime2 + tolerance) + ", but was " + maxValue,
                maxValue <= sleepTime2 + tolerance);

        double latency = histogram.getMean();
        assertTrue("latency should be >= " + sleepTime1 + ", but was " + latency, latency >= sleepTime1);
        assertTrue("latency should be < " + sleepTime2 + tolerance + ", but was " + latency, latency < sleepTime2 + tolerance);
    }
}
