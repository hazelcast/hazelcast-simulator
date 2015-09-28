package com.hazelcast.simulator.probes.probes.impl;

import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static com.hazelcast.simulator.probes.probes.impl.ProbeTestUtils.assertDisable;
import static com.hazelcast.simulator.probes.probes.impl.ProbeTestUtils.assertResult;
import static com.hazelcast.simulator.utils.ReflectionUtils.getObjectFromField;
import static com.hazelcast.simulator.utils.TestUtils.assertEqualsStringFormat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class ThroughputProbeTest {

    private ThroughputProbe throughputProbe = new ThroughputProbe();

    @Test
    public void testDisable() {
        assertDisable(throughputProbe);
    }

    @Test
    public void testStarted() {
        throughputProbe.started();
    }

    @Test
    public void testInvocationCount() {
        throughputProbe.done();
        throughputProbe.done();
        throughputProbe.done();

        assertEquals(3, throughputProbe.getInvocationCount());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testRecordValue() {
        throughputProbe.recordValue(500);
    }

    @Test(expected = IllegalStateException.class)
    public void testStopProbingWithoutInitialization() {
        throughputProbe.stopProbing(0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testStopProbingNegativeTimeStamp() {
        throughputProbe.stopProbing(-1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testStopProbingNegativeDuration() {
        throughputProbe.startProbing(1000);
        throughputProbe.stopProbing(999);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetValues_ZeroDuration() {
        throughputProbe.setValues(0, 10000);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetValues_ZeroInvocations() {
        throughputProbe.setValues(10000, 0);
    }

    @Test
    public void testSetValues_10ops() {
        throughputProbe.setValues(1000, 10);

        ThroughputResult result = throughputProbe.getResult();
        assertTrue(result != null);

        Long invocations = getObjectFromField(result, "invocations");
        assertEqualsStringFormat("Expected %d invocations, but was %d", 10L, invocations);

        Double operationsPerSecond = getObjectFromField(result, "operationsPerSecond");
        assertEqualsStringFormat("Expected %.2f op/s, but was %.2f", 10.0, operationsPerSecond, 0.01);
    }

    @Test
    public void testSetValues_20ops() {
        throughputProbe.setValues(500, 10);

        ThroughputResult result = throughputProbe.getResult();
        assertTrue(result != null);

        Long invocations = getObjectFromField(result, "invocations");
        assertEqualsStringFormat("Expected %d invocations, but was %d", 10L, invocations);

        Double operationsPerSecond = getObjectFromField(result, "operationsPerSecond");
        assertEqualsStringFormat("Expected %.2f op/s, but was %.2f", 20.0, operationsPerSecond, 0.01);
    }

    @Test
    public void testSetValues_800ops() {
        throughputProbe.setValues(5, 4);

        ThroughputResult result = throughputProbe.getResult();
        assertTrue(result != null);

        Long invocations = getObjectFromField(result, "invocations");
        assertEqualsStringFormat("Expected %d invocations, but was %d", 4L, invocations);

        Double operationsPerSecond = getObjectFromField(result, "operationsPerSecond");
        assertEqualsStringFormat("Expected %.2f op/s, but was %.2f", 800.0, operationsPerSecond, 0.01);
    }

    @Test
    public void testSetValues_8ops() {
        throughputProbe.setValues(500, 4);

        ThroughputResult result = throughputProbe.getResult();
        assertTrue(result != null);

        Long invocations = getObjectFromField(result, "invocations");
        assertEqualsStringFormat("Expected %d invocations, but was %d", 4L, invocations);

        Double operationsPerSecond = getObjectFromField(result, "operationsPerSecond");
        assertEqualsStringFormat("Expected %.2f op/s, but was %.2f", 8.0, operationsPerSecond, 0.01);
    }

    @Test
    public void testResult() {
        long started = System.currentTimeMillis();
        throughputProbe.startProbing(started);
        throughputProbe.done();
        throughputProbe.done();
        throughputProbe.done();
        throughputProbe.done();
        throughputProbe.stopProbing(started + TimeUnit.SECONDS.toMillis(5));

        ThroughputResult result = throughputProbe.getResult();
        assertTrue(result != null);

        Long invocations = getObjectFromField(result, "invocations");
        assertEqualsStringFormat("Expected %d invocations, but was %d", 4L, invocations);

        Double operationsPerSecond = getObjectFromField(result, "operationsPerSecond");
        assertEqualsStringFormat("Expected %.2f op/s, but was %.2f", 0.8, operationsPerSecond, 0.01);

        ThroughputProbe nonEqualsProbe = new ThroughputProbe();
        assertResult(result, nonEqualsProbe.getResult());
        nonEqualsProbe.setValues(12345, 4);
        assertResult(result, nonEqualsProbe.getResult());
    }

    @Test
    public void testResultCombine() {
        long started = System.currentTimeMillis();
        throughputProbe.startProbing(started);
        throughputProbe.done();
        throughputProbe.done();
        throughputProbe.stopProbing(started + TimeUnit.SECONDS.toMillis(5));

        ThroughputResult result1 = throughputProbe.getResult();
        assertTrue(result1 != null);

        throughputProbe.startProbing(started + TimeUnit.SECONDS.toMillis(10));
        throughputProbe.done();
        throughputProbe.done();
        throughputProbe.stopProbing(started + TimeUnit.SECONDS.toMillis(15));

        ThroughputResult result2 = throughputProbe.getResult();
        assertTrue(result2 != null);

        assertNotEquals(result1.hashCode(), result2.hashCode());

        ThroughputResult combined = result1.combine(result2);
        assertTrue(combined != null);

        Long invocations = getObjectFromField(combined, "invocations");
        assertEqualsStringFormat("Expected %d invocations, but was %d", 6L, invocations);

        Double operationsPerSecond = getObjectFromField(combined, "operationsPerSecond");
        assertEqualsStringFormat("Expected %.2f op/s, but was %.2f", 1.2, operationsPerSecond, 0.01);
    }
}
