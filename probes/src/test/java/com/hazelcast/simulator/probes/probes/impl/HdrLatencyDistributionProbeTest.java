package com.hazelcast.simulator.probes.probes.impl;

import org.HdrHistogram.Histogram;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class HdrLatencyDistributionProbeTest extends AbstractProbeTest {

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
    public void testResult() {
        long sleepTime = 150;
        long minLatency = TimeUnit.MILLISECONDS.toMillis(sleepTime);
        long maxLatency = TimeUnit.MILLISECONDS.toMillis(1000);

        hdrLatencyDistributionProbe.started();
        sleepNanos(TimeUnit.MILLISECONDS.toNanos(sleepTime));
        hdrLatencyDistributionProbe.done();

        HdrLatencyDistributionResult result = hdrLatencyDistributionProbe.getResult();
        assertTrue(result != null);

        Histogram histogram = result.getHistogram();
        assertEqualsStringFormat("Expected %d records, but was %d", 1L, histogram.getTotalCount());

        long minValue = histogram.getMinValue();
        assertEqualsStringFormat("Expected minValue %d to be the same as maxValue %d", minValue, histogram.getMaxValue());

        double latency = histogram.getMean();
        assertTrue("latency should be >= " + minLatency + ", but was " + latency, latency >= minLatency);
        assertTrue("latency should be < " + maxLatency + ", but was " + latency, latency < maxLatency);
    }

    @Test
    public void testResultToHumanString() {
        hdrLatencyDistributionProbe.started();
        sleepNanos(TimeUnit.MILLISECONDS.toNanos(150));
        hdrLatencyDistributionProbe.done();

        HdrLatencyDistributionResult result = hdrLatencyDistributionProbe.getResult();
        assertTrue(result != null);
        assertTrue(result.toHumanString() != null);
    }

    @Test
    public void testResultCombine() {
        long sleepTime1 = 150;
        long sleepTime2 = 300;
        long safetyMargin = 100;
        long latency1 = TimeUnit.MILLISECONDS.toMillis(sleepTime1);
        long latency2 = TimeUnit.MILLISECONDS.toMillis(sleepTime2);
        long maxLatency = TimeUnit.MILLISECONDS.toMillis(sleepTime2 + safetyMargin);

        hdrLatencyDistributionProbe.started();
        sleepNanos(TimeUnit.MILLISECONDS.toNanos(sleepTime1));
        hdrLatencyDistributionProbe.done();

        HdrLatencyDistributionResult result1 = hdrLatencyDistributionProbe.getResult();
        assertTrue(result1 != null);

        hdrLatencyDistributionProbe.started();
        sleepNanos(TimeUnit.MILLISECONDS.toNanos(sleepTime2));
        hdrLatencyDistributionProbe.done();

        HdrLatencyDistributionResult result2 = hdrLatencyDistributionProbe.getResult();
        assertTrue(result2 != null);

        HdrLatencyDistributionResult combined = result1.combine(result2);
        assertTrue(combined != null);

        Histogram histogram = combined.getHistogram();
        assertEqualsStringFormat("Expected %d records, but was %d", 2L, histogram.getTotalCount());

        long minValue = histogram.getMinValue();
        long maxValue = histogram.getMaxValue();
        assertNotEquals("Expected minValue and maxValue to differ", minValue, maxValue);
        assertTrue("Expected minValue >= " + (latency1 - 10) + ", but was " + minValue, minValue >= latency1 - 10);
        assertTrue("Expected minValue <= " + (latency1 + 10) + ", but was " + minValue, minValue <= latency1 + 10);
        assertTrue("Expected maxValue >= " + (latency2 - 10) + ", but was " + maxValue, maxValue >= latency2 - 10);
        assertTrue("Expected maxValue <= " + (latency2 + 10) + ", but was " + maxValue, maxValue <= latency2 + 10);

        double latency = histogram.getMean();
        assertTrue("latency should be >= " + latency1 + ", but was " + latency, latency >= latency1);
        assertTrue("latency should be < " + maxLatency + ", but was " + latency, latency < maxLatency);
    }
}