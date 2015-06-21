package com.hazelcast.simulator.probes.probes.impl;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class WorkerProbeTest {

    private WorkerProbe workerProbe = new WorkerProbe();

    @Test
    public void testStartProbing() {
        workerProbe.startProbing(0);
    }

    @Test
    public void testStopProbing() {
        workerProbe.stopProbing(0);
    }

    @Test
    public void testInvocations() {
        workerProbe.done();
        workerProbe.done();
        workerProbe.done();

        assertEquals(3, workerProbe.getInvocationCount());
    }

    @Test
    public void testRecordValue() {
        workerProbe.recordValue(-1);
        workerProbe.recordValue(5);

        assertEquals(2, workerProbe.getInvocationCount());
    }

    @Test
    public void testResult() {
        workerProbe.done();

        assertEquals(1, workerProbe.getInvocationCount());
        assertTrue(workerProbe.getResult() != null);
    }

    @Test
    public void testResult_withSetValues() {
        workerProbe.setValues(123914, 12932);

        assertEquals(12932, workerProbe.getInvocationCount());
        assertTrue(workerProbe.getResult() != null);
    }

    @Test
    public void testResult_withRecordValue() {
        workerProbe.recordValue(152);

        assertEquals(1, workerProbe.getInvocationCount());
        assertTrue(workerProbe.getResult() != null);
    }

    @Test
    public void testDisable() {
        assertTrue(workerProbe.isDisabled());

        workerProbe.disable();

        assertTrue(workerProbe.isDisabled());
    }
}
