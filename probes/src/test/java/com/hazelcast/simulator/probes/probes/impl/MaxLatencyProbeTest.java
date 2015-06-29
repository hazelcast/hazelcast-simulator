package com.hazelcast.simulator.probes.probes.impl;

import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static com.hazelcast.simulator.probes.probes.impl.ProbeTestUtils.TOLERANCE_MILLIS;
import static com.hazelcast.simulator.probes.probes.impl.ProbeTestUtils.assertDisable;
import static com.hazelcast.simulator.probes.probes.impl.ProbeTestUtils.assertResult;
import static com.hazelcast.simulator.utils.CommonUtils.sleepNanos;
import static com.hazelcast.simulator.utils.ReflectionUtils.getObjectFromField;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class MaxLatencyProbeTest {

    private MaxLatencyProbe maxLatencyProbe = new MaxLatencyProbe();

    @Test
    public void testDisable() {
        assertDisable(maxLatencyProbe);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSetValues() {
        maxLatencyProbe.setValues(123, 125812);
    }

    @Test(expected = IllegalStateException.class)
    public void testDoneWithoutStarted() {
        maxLatencyProbe.done();
    }

    @Test
    public void testInvocationCount() {
        maxLatencyProbe.started();
        maxLatencyProbe.done();
        maxLatencyProbe.done();
        maxLatencyProbe.done();
        maxLatencyProbe.done();
        maxLatencyProbe.done();

        assertEquals(5, maxLatencyProbe.getInvocationCount());
    }

    @Test
    public void testStartedDone() {
        long maxLatencyValue = 150;

        maxLatencyProbe.started();
        sleepNanos(TimeUnit.MILLISECONDS.toNanos(maxLatencyValue));
        maxLatencyProbe.done();

        MaxLatencyResult result = maxLatencyProbe.getResult();
        assertResult(result, new MaxLatencyProbe().getResult());

        Long maxLatencyMs = getObjectFromField(result, "maxLatencyMs");
        assertTrue(maxLatencyMs >= maxLatencyValue);
        assertTrue(maxLatencyMs < maxLatencyValue + TOLERANCE_MILLIS);
    }

    @Test
    public void testRecordValues() {
        long maxLatencyValue = 1000;

        maxLatencyProbe.recordValue(TimeUnit.MILLISECONDS.toNanos(500));
        maxLatencyProbe.recordValue(TimeUnit.MILLISECONDS.toNanos(maxLatencyValue));
        maxLatencyProbe.recordValue(TimeUnit.MILLISECONDS.toNanos(200));

        MaxLatencyResult result = maxLatencyProbe.getResult();
        assertResult(result, new MaxLatencyProbe().getResult());
        assertMaxLatency(result, maxLatencyValue);
    }

    @Test
    public void testResultCombine() {
        long latencyValue = 150;
        long maxLatencyValue = 300;

        maxLatencyProbe.recordValue(TimeUnit.MILLISECONDS.toNanos(latencyValue));
        MaxLatencyResult result1 = maxLatencyProbe.getResult();
        assertTrue(result1 != null);

        maxLatencyProbe.recordValue(TimeUnit.MILLISECONDS.toNanos(maxLatencyValue));
        MaxLatencyResult result2 = maxLatencyProbe.getResult();
        assertTrue(result2 != null);

        assertNotEquals(result1.hashCode(), result2.hashCode());

        MaxLatencyResult combined = result1.combine(result2);
        assertResult(combined, new MaxLatencyProbe().getResult());
        assertMaxLatency(combined, maxLatencyValue);
    }

    private static void assertMaxLatency(MaxLatencyResult result, long expectedMaxLatency) {
        Long maxLatencyMs = getObjectFromField(result, "maxLatencyMs");
        assertNotNull(maxLatencyMs);
        assertEquals(expectedMaxLatency, maxLatencyMs.longValue());
    }
}
