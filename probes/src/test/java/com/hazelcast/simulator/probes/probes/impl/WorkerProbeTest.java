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
    public void testRecordValues() {
        workerProbe.recordValue(-1);
        workerProbe.recordValue(5);

        assertEquals(2, workerProbe.getInvocationCount());
    }

    @Test
    public void testResult() {
        workerProbe.done();

        assertTrue(workerProbe.getResult() != null);
    }

    @Test
    public void testResult_withRecordValues() {
        workerProbe.recordValue(152);

        assertTrue(workerProbe.getResult() != null);
    }
}
