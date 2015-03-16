package com.hazelcast.simulator.probes.probes.impl;

import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MaxLatencyProbeTest extends AbstractProbeTest {

    private MaxLatencyProbe maxLatencyProbe = new MaxLatencyProbe();

    @Test(expected = IllegalStateException.class)
    public void testDoneWithoutStarted() {
        maxLatencyProbe.done();
    }

    @Test
    public void testInvocations() {
        maxLatencyProbe.started();
        maxLatencyProbe.done();
        maxLatencyProbe.done();
        maxLatencyProbe.done();
        maxLatencyProbe.done();
        maxLatencyProbe.done();

        assertEquals(5, maxLatencyProbe.getInvocationCount());
    }

    @Test
    public void testResult() {
        maxLatencyProbe.started();
        sleepNanos(TimeUnit.MILLISECONDS.toNanos(150));
        maxLatencyProbe.done();

        MaxLatencyResult result = maxLatencyProbe.getResult();
        assertTrue(result != null);

        Long maxLatencyMs = getObjectFromField(result, "maxLatencyMs");
        assertTrue(maxLatencyMs >= 150);
        assertTrue(maxLatencyMs < 1000);
    }

    @Test
    public void testResultToHumanString() {
        maxLatencyProbe.started();
        sleepNanos(TimeUnit.MILLISECONDS.toNanos(150));
        maxLatencyProbe.done();

        MaxLatencyResult result = maxLatencyProbe.getResult();
        assertTrue(result != null);
        assertTrue(result.toHumanString() != null);
    }

    @Test
    public void testResultCombine() {
        maxLatencyProbe.started();
        sleepNanos(TimeUnit.MILLISECONDS.toNanos(150));
        maxLatencyProbe.done();

        MaxLatencyResult result1 = maxLatencyProbe.getResult();
        assertTrue(result1 != null);

        maxLatencyProbe.started();
        sleepNanos(TimeUnit.MILLISECONDS.toNanos(300));
        maxLatencyProbe.done();

        MaxLatencyResult result2 = maxLatencyProbe.getResult();
        assertTrue(result2 != null);

        MaxLatencyResult combined = result1.combine(result2);
        assertTrue(combined != null);

        Long maxLatencyMs = getObjectFromField(combined, "maxLatencyMs");
        assertTrue(maxLatencyMs >= 300);
        assertTrue(maxLatencyMs < 1000);
    }
}