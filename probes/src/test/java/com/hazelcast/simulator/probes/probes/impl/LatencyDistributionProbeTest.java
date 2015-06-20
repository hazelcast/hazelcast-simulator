package com.hazelcast.simulator.probes.probes.impl;

import com.hazelcast.simulator.probes.probes.LinearHistogram;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static com.hazelcast.simulator.utils.CommonUtils.sleepNanos;
import static com.hazelcast.simulator.utils.TestUtils.assertEqualsStringFormat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class LatencyDistributionProbeTest {

    private LatencyDistributionProbe latencyDistributionProbe = new LatencyDistributionProbe();

    @Test(expected = IllegalStateException.class)
    public void testDoneWithoutStarted() {
        latencyDistributionProbe.done();
    }

    @Test
    public void testInvocations() {
        latencyDistributionProbe.started();
        latencyDistributionProbe.done();
        latencyDistributionProbe.done();
        latencyDistributionProbe.done();
        latencyDistributionProbe.done();
        latencyDistributionProbe.done();

        assertEquals(5, latencyDistributionProbe.getInvocationCount());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSetValues() {
        latencyDistributionProbe.setValues(123, 125812);
    }

    @Test
    public void testRecordValue() {
        latencyDistributionProbe.recordValue(TimeUnit.MILLISECONDS.toNanos(500));
        latencyDistributionProbe.recordValue(TimeUnit.MILLISECONDS.toNanos(200));
        latencyDistributionProbe.recordValue(TimeUnit.MILLISECONDS.toNanos(1000));

        assertEquals(3, latencyDistributionProbe.getInvocationCount());
    }

    @Test
    public void testResult() {
        long sleepTime = 150;
        long minLatency = TimeUnit.MILLISECONDS.toMicros(sleepTime) / 10;
        long maxLatency = TimeUnit.MILLISECONDS.toMicros(1000) / 10;

        latencyDistributionProbe.started();
        sleepNanos(TimeUnit.MILLISECONDS.toNanos(sleepTime));
        latencyDistributionProbe.done();

        LatencyDistributionResult result = latencyDistributionProbe.getResult();
        assertTrue(result != null);

        int foundBuckets = 0;
        LinearHistogram linearHistogram = result.getHistogram();
        int[] buckets = linearHistogram.getBuckets();
        for (int latency = 0; latency < buckets.length; latency++) {
            if (buckets[latency] == 1) {
                assertTrue("latency should be >= " + minLatency + ", but was " + latency, latency >= minLatency);
                assertTrue("latency should be <= " + maxLatency + ", but was " + latency, latency <= maxLatency);
                foundBuckets++;
            }
        }
        assertEqualsStringFormat("Expected to find %d buckets with latency info, but found %d", 1, foundBuckets);
    }

    @Test
    public void testResult_withRecordValue() {
        long minLatency = TimeUnit.MILLISECONDS.toMicros(200) / 10;
        long maxLatency = TimeUnit.MILLISECONDS.toMicros(1000) / 10;

        latencyDistributionProbe.recordValue(TimeUnit.MILLISECONDS.toNanos(500));
        latencyDistributionProbe.recordValue(TimeUnit.MILLISECONDS.toNanos(200));
        latencyDistributionProbe.recordValue(TimeUnit.MILLISECONDS.toNanos(1000));

        LatencyDistributionResult result = latencyDistributionProbe.getResult();
        assertTrue(result != null);

        int foundBuckets = 0;
        LinearHistogram linearHistogram = result.getHistogram();
        int[] buckets = linearHistogram.getBuckets();
        for (int latency = 0; latency < buckets.length; latency++) {
            if (buckets[latency] == 1) {
                assertTrue("latency should be >= " + minLatency + ", but was " + latency, latency >= minLatency);
                assertTrue("latency should be <= " + maxLatency + ", but was " + latency, latency <= maxLatency);
                foundBuckets++;
            }
        }
        assertEqualsStringFormat("Expected to find %d buckets with latency info, but found %d", 3, foundBuckets);
    }

    @Test
    public void testResultToHumanString() {
        latencyDistributionProbe.started();
        sleepNanos(TimeUnit.MILLISECONDS.toNanos(150));
        latencyDistributionProbe.done();

        LatencyDistributionResult result = latencyDistributionProbe.getResult();
        assertTrue(result != null);
        assertTrue(result.toHumanString() != null);
    }

    @Test
    public void testResultCombine() {
        long sleepTime1 = 150;
        long sleepTime2 = 300;
        long safetyMargin = 100;
        long minLatency = TimeUnit.MILLISECONDS.toMicros(sleepTime1) / 10;
        long maxLatency = TimeUnit.MILLISECONDS.toMicros(sleepTime2 + safetyMargin) / 10;

        latencyDistributionProbe.started();
        sleepNanos(TimeUnit.MILLISECONDS.toNanos(sleepTime1));
        latencyDistributionProbe.done();

        LatencyDistributionResult result1 = latencyDistributionProbe.getResult();
        assertTrue(result1 != null);

        latencyDistributionProbe.started();
        sleepNanos(TimeUnit.MILLISECONDS.toNanos(sleepTime2));
        latencyDistributionProbe.done();

        LatencyDistributionResult result2 = latencyDistributionProbe.getResult();
        assertTrue(result2 != null);

        LatencyDistributionResult combined = result1.combine(result2);
        assertTrue(combined != null);

        int foundBuckets = 0;
        LinearHistogram linearHistogram = combined.getHistogram();
        int[] buckets = linearHistogram.getBuckets();
        for (int latency = 0; latency < buckets.length; latency++) {
            if (buckets[latency] > 0) {
                assertTrue("latency should be >= " + minLatency + ", but was " + latency, latency >= minLatency);
                assertTrue("latency should be < " + maxLatency + ", but was " + latency, latency <= maxLatency);
                foundBuckets++;
            }
        }
        assertEqualsStringFormat("Expected to find %d buckets with latency info, but found %d", 2, foundBuckets);
    }
}
