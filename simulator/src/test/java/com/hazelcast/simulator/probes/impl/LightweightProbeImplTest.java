package com.hazelcast.simulator.probes.impl;

import com.hazelcast.simulator.probes.Probe;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static com.hazelcast.simulator.utils.CommonUtils.sleepNanos;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class LightweightProbeImplTest {

    private LightweightProbeImpl probe = new LightweightProbeImpl(false);

    @Test
    public void testConstructor_throughputProbe() {
        Probe tmpProbe = new LightweightProbeImpl(true);
        assertTrue(tmpProbe.isThroughputProbe());
    }

    @Test
    public void testConstructor_noThroughputProbe() {
        Probe tmpProbe = new LightweightProbeImpl(false);
        assertFalse(tmpProbe.isThroughputProbe());
    }

    @Test
    public void testIsLightWeightProbe() {
        assertTrue(probe.isLightweightProbe());
    }

    @Test
    public void testDone_withStarted() {
        int expectedCount = 1;
        long expectedLatency = 150;

        probe.started();
        sleepNanos(TimeUnit.MILLISECONDS.toNanos(expectedLatency));
        probe.done();

        assertEquals(expectedCount, probe.getIntervalCountAndReset());
        assertEquals(0, probe.getIntervalCountAndReset());
    }

    @Test
    public void testDone_withExternalStarted() {
        int expectedCount = 1;
        long expectedLatency = 150;

        long started = System.nanoTime();
        sleepNanos(TimeUnit.MILLISECONDS.toNanos(expectedLatency));
        probe.done(started);

        assertEquals(expectedCount, probe.getIntervalCountAndReset());
        assertEquals(0, probe.getIntervalCountAndReset());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDone_withExternalStarted_withZero() {
        probe.done(0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDone_withExternalStarted_withNegativeValue() {
        probe.done(-23);
    }

    @Test
    public void testRecordValues() {
        int expectedCount = 3;
        long latencyValue = 500;
        long expectedMinValue = 200;
        long expectedMaxValue = 1000;

        probe.recordValue(TimeUnit.MILLISECONDS.toNanos(latencyValue));
        probe.recordValue(TimeUnit.MILLISECONDS.toNanos(expectedMinValue));
        probe.recordValue(TimeUnit.MILLISECONDS.toNanos(expectedMaxValue));

        assertEquals(expectedCount, probe.getIntervalCountAndReset());
        assertEquals(0, probe.getIntervalCountAndReset());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetIntervalHistogram()  {
        probe.getIntervalHistogram();
    }
}
