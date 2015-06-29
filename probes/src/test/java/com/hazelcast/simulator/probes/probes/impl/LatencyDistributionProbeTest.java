package com.hazelcast.simulator.probes.probes.impl;

import com.hazelcast.simulator.probes.probes.LinearHistogram;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static com.hazelcast.simulator.probes.probes.impl.ProbeTestUtils.TOLERANCE_MILLIS;
import static com.hazelcast.simulator.probes.probes.impl.ProbeTestUtils.assertDisable;
import static com.hazelcast.simulator.probes.probes.impl.ProbeTestUtils.assertResult;
import static com.hazelcast.simulator.utils.CommonUtils.sleepNanos;
import static com.hazelcast.simulator.utils.TestUtils.assertEqualsStringFormat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class LatencyDistributionProbeTest {

    private LatencyDistributionProbe latencyDistributionProbe = new LatencyDistributionProbe();

    @Test
    public void testDisable() {
        assertDisable(latencyDistributionProbe);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSetValues() {
        latencyDistributionProbe.setValues(123, 125812);
    }

    @Test(expected = IllegalStateException.class)
    public void testDoneWithoutStarted() {
        latencyDistributionProbe.done();
    }

    @Test
    public void testInvocationCount() {
        latencyDistributionProbe.started();
        latencyDistributionProbe.done();
        latencyDistributionProbe.done();
        latencyDistributionProbe.done();
        latencyDistributionProbe.done();
        latencyDistributionProbe.done();

        assertEquals(5, latencyDistributionProbe.getInvocationCount());
    }

    @Test
    public void testStartedDone() {
        int expectedCount = 1;
        long expectedLatency = 150;

        latencyDistributionProbe.started();
        sleepNanos(TimeUnit.MILLISECONDS.toNanos(expectedLatency));
        latencyDistributionProbe.done();

        LatencyDistributionResult result = latencyDistributionProbe.getResult();
        assertResult(result, new LatencyDistributionProbe().getResult());
        assertHistogram(result, expectedCount, expectedLatency, expectedLatency + TOLERANCE_MILLIS);
    }

    @Test
    public void testRecordValues() {
        int expectedCount = 3;
        long latencyValue = 500;
        long expectedMinLatency = 200;
        long expectedMaxLatency = 1000;

        latencyDistributionProbe.recordValue(TimeUnit.MILLISECONDS.toNanos(latencyValue));
        latencyDistributionProbe.recordValue(TimeUnit.MILLISECONDS.toNanos(expectedMaxLatency));
        latencyDistributionProbe.recordValue(TimeUnit.MILLISECONDS.toNanos(expectedMinLatency));

        LatencyDistributionResult result = latencyDistributionProbe.getResult();
        assertResult(result, new LatencyDistributionProbe().getResult());
        assertHistogram(result, expectedCount, expectedMinLatency, expectedMaxLatency);
    }

    @Test
    public void testResultCombine() {
        int expectedCount = 2;
        long expectedMinLatency = 150;
        long expectedMaxLatency = 300;

        latencyDistributionProbe.recordValue(TimeUnit.MILLISECONDS.toNanos(expectedMinLatency));

        LatencyDistributionResult result1 = latencyDistributionProbe.getResult();
        assertTrue(result1 != null);

        latencyDistributionProbe = new LatencyDistributionProbe();
        latencyDistributionProbe.recordValue(TimeUnit.MILLISECONDS.toNanos(expectedMaxLatency));

        LatencyDistributionResult result2 = latencyDistributionProbe.getResult();
        assertTrue(result2 != null);

        assertNotEquals(result1.hashCode(), result2.hashCode());

        LatencyDistributionResult combined = result1.combine(result2);
        assertResult(combined, new LatencyDistributionProbe().getResult());
        assertHistogram(combined, expectedCount, expectedMinLatency, expectedMaxLatency);
    }

    private static void assertHistogram(LatencyDistributionResult result, int expectedCount,
                                        long expectedMinLatencyMillis, long expectedMaxLatencyMillis) {
        long expectedMinLatency = TimeUnit.MILLISECONDS.toMicros(expectedMinLatencyMillis) / 10;
        long expectedMaxLatency = TimeUnit.MILLISECONDS.toMicros(expectedMaxLatencyMillis) / 10;

        int foundBuckets = 0;
        LinearHistogram linearHistogram = result.getHistogram();
        int[] buckets = linearHistogram.getBuckets();
        for (int latency = 0; latency < buckets.length; latency++) {
            if (buckets[latency] == 1) {
                assertTrue("latency should be >= " + expectedMinLatency + ", but was " + latency, latency >= expectedMinLatency);
                assertTrue("latency should be <= " + expectedMaxLatency + ", but was " + latency, latency <= expectedMaxLatency);
                foundBuckets++;
            }
        }
        assertEqualsStringFormat("Expected to find %d buckets with latency info, but found %d", expectedCount, foundBuckets);
    }
}
