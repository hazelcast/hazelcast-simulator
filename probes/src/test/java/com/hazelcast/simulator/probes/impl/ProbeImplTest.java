package com.hazelcast.simulator.probes.impl;

import com.hazelcast.simulator.probes.Probe;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static com.hazelcast.simulator.probes.ProbeTestUtils.assertHistogram;
import static com.hazelcast.simulator.utils.CommonUtils.sleepNanos;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ProbeImplTest {

    private ProbeImpl probe = new ProbeImpl(false);

    @Test
    public void testConstructor_throughputProbe() {
        Probe tmpProbe = new ProbeImpl(true);
        assertTrue(tmpProbe.isThroughputProbe());
    }

    @Test
    public void testConstructor_noThroughputProbe() {
        Probe tmpProbe = new ProbeImpl(false);
        assertFalse(tmpProbe.isThroughputProbe());
    }

    @Test
    public void testDone_withStarted() {
        int expectedCount = 1;
        long expectedLatency = 150;

        probe.started();
        sleepNanos(TimeUnit.MILLISECONDS.toNanos(expectedLatency));
        probe.done();

        assertHistogram(probe.getIntervalHistogram(), expectedCount, expectedLatency, expectedLatency, expectedLatency);
    }

    @Test(expected = IllegalStateException.class)
    public void testDone_withoutStarted() {
        probe.done();
    }

    @Test
    public void testRecordValues() {
        int expectedCount = 3;
        long latencyValue = 500;
        long expectedMinValue = 200;
        long expectedMaxValue = 1000;
        long expectedMeanValue = (long) ((latencyValue + expectedMinValue + expectedMaxValue) / (double) expectedCount);

        probe.recordValue(TimeUnit.MILLISECONDS.toNanos(latencyValue));
        probe.recordValue(TimeUnit.MILLISECONDS.toNanos(expectedMinValue));
        probe.recordValue(TimeUnit.MILLISECONDS.toNanos(expectedMaxValue));

        assertHistogram(probe.getIntervalHistogram(), expectedCount, expectedMinValue, expectedMaxValue, expectedMeanValue);
    }
}
