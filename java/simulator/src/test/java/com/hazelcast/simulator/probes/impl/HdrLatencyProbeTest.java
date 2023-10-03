package com.hazelcast.simulator.probes.impl;

import com.hazelcast.simulator.probes.LatencyProbe;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.HistogramIterationValue;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static com.hazelcast.simulator.probes.impl.HdrLatencyProbe.HIGHEST_TRACKABLE_VALUE_NANOS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class HdrLatencyProbeTest {

    private HdrLatencyProbe probe = new HdrLatencyProbe("foo", false);

    @Test
    public void testConstructor_throughputProbe() {
        LatencyProbe tmpProbe = new HdrLatencyProbe("foo", true);
        assertTrue(tmpProbe.includeInThroughput());
    }

    @Test
    public void testConstructor_noThroughputProbe() {
        LatencyProbe tmpProbe = new HdrLatencyProbe("foo", false);
        assertFalse(tmpProbe.includeInThroughput());
    }

    @Test
    public void testDone_withExternalStarted() throws InterruptedException {
        long expectedLatency = TimeUnit.SECONDS.toNanos(2);

        long started = System.nanoTime();

        TimeUnit.NANOSECONDS.sleep(expectedLatency);

        probe.done(started);

        Histogram histogram = probe.getRecorder().getIntervalHistogram();
        assertEquals(1, histogram.getTotalCount());

        HistogramIterationValue iterationValue = histogram.recordedValues().iterator().next();
        assertTrue(0.90 * iterationValue.getValueIteratedFrom() < expectedLatency);
        assertTrue(1.10 * iterationValue.getValueIteratedTo() > expectedLatency);
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
        long value1 = MILLISECONDS.toNanos(200);
        long value2 = MILLISECONDS.toNanos(500);
        long value3 = MILLISECONDS.toNanos(1000);

        probe.recordValue(value1);
        probe.recordValue(value2);
        probe.recordValue(value3);

        Histogram histogram = probe.getRecorder().getIntervalHistogram();
        assertHistogramContent(histogram, value1, value2, value3);
    }

    @Test
    public void testNegativeValue() {
        HdrLatencyProbe probe = new HdrLatencyProbe("foo", false);
        long value1 = MILLISECONDS.toNanos(-200);

        probe.recordValue(value1);

        assertEquals(1, probe.negativeCount());

        Histogram histogram = probe.getRecorder().getIntervalHistogram();
        assertHistogramContent(histogram, -value1);
    }

    @Test
    public void testLongMinValue() {
        HdrLatencyProbe probe = new HdrLatencyProbe("foo", false);

        long value1 = MILLISECONDS.toNanos(Long.MIN_VALUE);

        probe.recordValue(value1);

        assertEquals(1, probe.negativeCount());

        Histogram histogram = probe.getRecorder().getIntervalHistogram();
        assertHistogramContent(histogram, HIGHEST_TRACKABLE_VALUE_NANOS);
    }

    @Test
    public void testRecord_whenTooLarge() {
        long value = HIGHEST_TRACKABLE_VALUE_NANOS * 2;
        probe.recordValue(value);

        Histogram histogram = probe.getRecorder().getIntervalHistogram();
        assertHistogramContent(histogram, HIGHEST_TRACKABLE_VALUE_NANOS);
    }

    private void assertHistogramContent(Histogram histogram, long... requiredValues) {
        assertEquals(histogram.getTotalCount(), requiredValues.length);

        for (long requiredValue : requiredValues) {
            if (!contains(histogram, requiredValue)) {
                fail("Value " + requiredValue + " not found in histogram");
            }
        }
    }

    private boolean contains(Histogram histogram, long value) {
        for (HistogramIterationValue iterationValue : histogram.allValues()) {
            if (iterationValue.getTotalCountToThisValue() == 0) {
                continue;
            }

            long max = iterationValue.getValueIteratedTo();
            long min = iterationValue.getValueIteratedFrom();

            if (value >= min && value <= max) {
                return true;
            }

        }
        return false;
    }

    @Test
    public void testGet() {
        probe.recordValue(1);
        probe.recordValue(2);
        probe.recordValue(3);

        assertEquals(3, probe.getRecorder().getIntervalHistogram().getTotalCount());
    }


}
