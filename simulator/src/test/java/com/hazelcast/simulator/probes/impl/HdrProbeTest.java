package com.hazelcast.simulator.probes.impl;

import com.hazelcast.simulator.probes.Probe;
import org.junit.Test;

import static com.hazelcast.simulator.probes.ProbeTestUtils.assertHistogram;
import static com.hazelcast.simulator.utils.CommonUtils.sleepNanos;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class HdrProbeTest {

    private HdrProbe probe = new HdrProbe(false);

    @Test
    public void testConstructor_throughputProbe() {
        Probe tmpProbe = new HdrProbe(true);
        assertTrue(tmpProbe.isPartOfTotalThroughput());
    }

    @Test
    public void testConstructor_noThroughputProbe() {
        Probe tmpProbe = new HdrProbe(false);
        assertFalse(tmpProbe.isPartOfTotalThroughput());
    }

    @Test
    public void testIsMeasuringLatency() {
        assertTrue(probe instanceof HdrProbe);
    }

    @Test
    public void testDone_withExternalStarted() {
        int expectedCount = 1;
        long expectedLatency = 150;

        long started = System.nanoTime();
        sleepNanos(MILLISECONDS.toNanos(expectedLatency));
        probe.done(started);

        assertHistogram(probe.getIntervalHistogram(), expectedCount, expectedLatency, expectedLatency, expectedLatency);
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
        long expectedMeanValue = (long) ((latencyValue + expectedMinValue + expectedMaxValue) / (double) expectedCount);

        probe.recordValue(MILLISECONDS.toNanos(latencyValue));
        probe.recordValue(MILLISECONDS.toNanos(expectedMinValue));
        probe.recordValue(MILLISECONDS.toNanos(expectedMaxValue));

        assertHistogram(probe.getIntervalHistogram(), expectedCount, expectedMinValue, expectedMaxValue, expectedMeanValue);
    }

    @Test
    public void testGet()  {
        probe.recordValue(1);
        probe.recordValue(2);
        probe.recordValue(3);

        assertEquals(3, probe.get());
    }
}
