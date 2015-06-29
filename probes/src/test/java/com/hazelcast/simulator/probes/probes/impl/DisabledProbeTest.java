package com.hazelcast.simulator.probes.probes.impl;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class DisabledProbeTest {

    private DisabledProbe disabledProbe = DisabledProbe.INSTANCE;

    @Test
    public void testDisable() {
        assertTrue(disabledProbe.isDisabled());

        disabledProbe.disable();

        assertTrue(disabledProbe.isDisabled());
    }

    @Test
    public void testSetValues() {
        disabledProbe.setValues(123, 1254);

        assertEquals(0, disabledProbe.getInvocationCount());
    }

    @Test
    public void testStartProbing() {
        disabledProbe.startProbing(0);
    }

    @Test
    public void testStopProbing() {
        disabledProbe.stopProbing(0);
    }

    @Test
    public void testStarted() {
        disabledProbe.started();
    }

    @Test
    public void testInvocationCount() {
        disabledProbe.done();
        disabledProbe.done();
        disabledProbe.done();

        assertEquals(0, disabledProbe.getInvocationCount());
    }

    @Test
    public void testRecordValue() {
        disabledProbe.recordValue(-1);
        disabledProbe.recordValue(500);

        assertEquals(0, disabledProbe.getInvocationCount());
    }

    @Test
    public void testResultToHumanString() {
        DisabledResult result = disabledProbe.getResult();
        assertTrue(result != null);
        assertNotNull(result.toHumanString());
    }

    @Test
    public void testResult() {
        disabledProbe.done();

        assertTrue(disabledProbe.getResult() != null);
    }

    @Test
    public void testResult_withRecordValue() {
        disabledProbe.recordValue(152);

        assertTrue(disabledProbe.getResult() != null);
    }

    @Test
    public void testResultCombine() {
        DisabledResult result = disabledProbe.getResult();
        assertTrue(result != null);

        DisabledResult combined = result.combine(null);
        assertTrue(combined != null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testResultWriteTo() {
        DisabledResult result = disabledProbe.getResult();
        result.writeTo(null);
    }

    @Test
    public void testResultHashCode() {
        assertEquals(1, disabledProbe.getResult().hashCode());
    }

    @Test
    public void testResultEquals() {
        assertTrue(disabledProbe.getResult().equals(DisabledProbe.INSTANCE.getResult()));
    }
}
