package com.hazelcast.simulator.probes.probes.impl;

import com.hazelcast.simulator.probes.probes.Result;
import org.HdrHistogram.Histogram;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static com.hazelcast.simulator.probes.probes.impl.ProbeTestUtils.TOLERANCE_MILLIS;
import static com.hazelcast.simulator.probes.probes.impl.ProbeTestUtils.assertDisable;
import static com.hazelcast.simulator.probes.probes.impl.ProbeTestUtils.assertResult;
import static com.hazelcast.simulator.probes.probes.impl.ProbeTestUtils.assertWithinTolerance;
import static com.hazelcast.simulator.utils.CommonUtils.sleepNanos;
import static com.hazelcast.simulator.utils.TestUtils.assertEqualsStringFormat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class ProbeImplTest {

    private ProbeImpl probe = new ProbeImpl();

    @Test
    public void testDisable() {
        assertDisable(probe);
    }

    @Test
    public void testSetValues() {
        probe.setValues(2000, 125000);

        Result result = probe.getResult();

        assertEquals(125000, result.getInvocationCount());
        assertEquals(62500d, result.getThroughput(), 0.001);
    }

    @Test(expected = IllegalStateException.class)
    public void testDoneWithoutStarted() {
        probe.done();
    }

    @Test
    public void testInvocationCount() {
        probe.started();
        probe.done();
        probe.done();
        probe.done();
        probe.done();
        probe.done();

        assertEquals(5, probe.getInvocationCount());
    }

    @Test
    public void testStartedDone() {
        int expectedCount = 1;
        long expectedLatency = 150;

        probe.started();
        sleepNanos(TimeUnit.MILLISECONDS.toNanos(expectedLatency));
        probe.done();

        ResultImpl result = probe.getResult();
        assertResult(result, new ProbeImpl().getResult());
        assertHistogram(result.getHistogram(), expectedCount, expectedLatency, expectedLatency, expectedLatency);
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

        ResultImpl result = probe.getResult();
        assertResult(result, new ProbeImpl().getResult());
        assertHistogram(result.getHistogram(), expectedCount, expectedMinValue, expectedMaxValue, expectedMeanValue);
    }

    @Test
    public void testResultCombine() {
        int expectedCount = 2;
        long expectedMinValue = 150;
        long expectedMaxValue = 500;
        long expectedMeanValue = (long) ((expectedMinValue + expectedMaxValue) / (double) expectedCount);

        probe.recordValue(TimeUnit.MILLISECONDS.toNanos(expectedMinValue));

        ResultImpl result1 = probe.getResult();
        assertSingleResult(result1);

        ProbeImpl probe2 = new ProbeImpl();
        probe2.recordValue(TimeUnit.MILLISECONDS.toNanos(expectedMaxValue));

        ResultImpl result2 = probe2.getResult();
        assertSingleResult(result2);

        assertNotEquals(result1.hashCode(), result2.hashCode());

        ResultImpl combined = (ResultImpl) result1.combine(result2);
        assertResult(combined, new ProbeImpl().getResult());
        assertHistogram(combined.getHistogram(), expectedCount, expectedMinValue, expectedMaxValue, expectedMeanValue);
    }

    private static void assertSingleResult(ResultImpl result) {
        assertTrue(result != null);
        assertEqualsStringFormat("Expected %d records, but was %d", 1L, result.getHistogram().getTotalCount());
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
}
