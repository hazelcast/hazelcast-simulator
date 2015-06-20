package com.hazelcast.simulator.probes.probes.impl;

import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static com.hazelcast.simulator.utils.ReflectionUtils.getObjectFromField;
import static com.hazelcast.simulator.utils.TestUtils.assertEqualsStringFormat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class OperationsPerSecProbeTest {

    private OperationsPerSecProbe operationsPerSecProbe = new OperationsPerSecProbe();

    @Test
    public void testStarted() {
        operationsPerSecProbe.started();
    }

    @Test
    public void testInvocations() {
        operationsPerSecProbe.done();
        operationsPerSecProbe.done();
        operationsPerSecProbe.done();

        assertEquals(3, operationsPerSecProbe.getInvocationCount());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testRecordValue() {
        operationsPerSecProbe.recordValue(500);
    }

    @Test(expected = IllegalStateException.class)
    public void testStopProbingWithoutInitialization() {
        operationsPerSecProbe.stopProbing(0);
    }

    @Test(expected = IllegalStateException.class)
    public void testResultWithoutInitialization() {
        operationsPerSecProbe.getResult();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetValues_ZeroDuration() {
        operationsPerSecProbe.setValues(0, 10000);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetValues_ZeroInvocations() {
        operationsPerSecProbe.setValues(10000, 0);
    }

    @Test
    public void testSetValues_10ops() {
        operationsPerSecProbe.setValues(1000, 10);

        OperationsPerSecResult result = operationsPerSecProbe.getResult();
        assertTrue(result != null);

        Long invocations = getObjectFromField(result, "invocations");
        assertEqualsStringFormat("Expected %d invocations, but was %d", 10L, invocations);

        Double operationsPerSecond = getObjectFromField(result, "operationsPerSecond");
        assertEqualsStringFormat("Expected %.2f op/s, but was %.2f", 10.0, operationsPerSecond, 0.01);
    }

    @Test
    public void testSetValues_20ops() {
        operationsPerSecProbe.setValues(500, 10);

        OperationsPerSecResult result = operationsPerSecProbe.getResult();
        assertTrue(result != null);

        Long invocations = getObjectFromField(result, "invocations");
        assertEqualsStringFormat("Expected %d invocations, but was %d", 10L, invocations);

        Double operationsPerSecond = getObjectFromField(result, "operationsPerSecond");
        assertEqualsStringFormat("Expected %.2f op/s, but was %.2f", 20.0, operationsPerSecond, 0.01);
    }

    @Test
    public void testSetValues_800ops() {
        operationsPerSecProbe.setValues(5, 4);

        OperationsPerSecResult result = operationsPerSecProbe.getResult();
        assertTrue(result != null);

        Long invocations = getObjectFromField(result, "invocations");
        assertEqualsStringFormat("Expected %d invocations, but was %d", 4L, invocations);

        Double operationsPerSecond = getObjectFromField(result, "operationsPerSecond");
        assertEqualsStringFormat("Expected %.2f op/s, but was %.2f", 800.0, operationsPerSecond, 0.01);
    }

    @Test
    public void testSetValues_8ops() {
        operationsPerSecProbe.setValues(500, 4);

        OperationsPerSecResult result = operationsPerSecProbe.getResult();
        assertTrue(result != null);

        Long invocations = getObjectFromField(result, "invocations");
        assertEqualsStringFormat("Expected %d invocations, but was %d", 4L, invocations);

        Double operationsPerSecond = getObjectFromField(result, "operationsPerSecond");
        assertEqualsStringFormat("Expected %.2f op/s, but was %.2f", 8.0, operationsPerSecond, 0.01);
    }

    @Test
    public void testResult() {
        long started = System.currentTimeMillis();
        operationsPerSecProbe.startProbing(started);
        operationsPerSecProbe.done();
        operationsPerSecProbe.done();
        operationsPerSecProbe.done();
        operationsPerSecProbe.done();
        operationsPerSecProbe.stopProbing(started + TimeUnit.SECONDS.toMillis(5));

        OperationsPerSecResult result = operationsPerSecProbe.getResult();
        assertTrue(result != null);

        Long invocations = getObjectFromField(result, "invocations");
        assertEqualsStringFormat("Expected %d invocations, but was %d", 4L, invocations);

        Double operationsPerSecond = getObjectFromField(result, "operationsPerSecond");
        assertEqualsStringFormat("Expected %.2f op/s, but was %.2f", 0.8, operationsPerSecond, 0.01);
    }

    @Test
    public void testResultToHumanString() {
        long started = System.currentTimeMillis();
        operationsPerSecProbe.startProbing(started);
        operationsPerSecProbe.done();
        operationsPerSecProbe.stopProbing(started + TimeUnit.SECONDS.toMillis(5));

        OperationsPerSecResult result = operationsPerSecProbe.getResult();
        assertTrue(result != null);
        assertTrue(result.toHumanString() != null);
    }

    @Test
    public void testResultCombine() {
        long started = System.currentTimeMillis();
        operationsPerSecProbe.startProbing(started);
        operationsPerSecProbe.done();
        operationsPerSecProbe.done();
        operationsPerSecProbe.stopProbing(started + TimeUnit.SECONDS.toMillis(5));

        OperationsPerSecResult result1 = operationsPerSecProbe.getResult();
        assertTrue(result1 != null);

        operationsPerSecProbe.startProbing(started + TimeUnit.SECONDS.toMillis(10));
        operationsPerSecProbe.done();
        operationsPerSecProbe.done();
        operationsPerSecProbe.stopProbing(started + TimeUnit.SECONDS.toMillis(15));

        OperationsPerSecResult result2 = operationsPerSecProbe.getResult();
        assertTrue(result2 != null);

        OperationsPerSecResult combined = result1.combine(result2);
        assertTrue(combined != null);

        Long invocations = getObjectFromField(combined, "invocations");
        assertEqualsStringFormat("Expected %d invocations, but was %d", 6L, invocations);

        Double operationsPerSecond = getObjectFromField(combined, "operationsPerSecond");
        assertEqualsStringFormat("Expected %.2f op/s, but was %.2f", 1.2, operationsPerSecond, 0.01);
    }
}
