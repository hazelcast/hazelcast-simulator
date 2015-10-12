package com.hazelcast.simulator.probes.probes.impl;

import com.hazelcast.simulator.probes.probes.Probe;
import org.HdrHistogram.Histogram;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static com.hazelcast.simulator.utils.CommonUtils.sleepNanos;
import static com.hazelcast.simulator.utils.TestUtils.assertEqualsStringFormat;
import static java.lang.String.format;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class ProbeImplTest {

    private static final int TOLERANCE_MILLIS = 500;

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

    private static void assertWithinTolerance(String fieldName, long expected, long actual, long tolerance) {
        assertTrue(format("Expected %s >= %d, but was %d", fieldName, expected - tolerance, actual),
                actual >= expected - tolerance);
        assertTrue(format("Expected %s <= %d, but was %d", fieldName, expected + tolerance, actual),
                actual <= expected + tolerance);
    }
}
