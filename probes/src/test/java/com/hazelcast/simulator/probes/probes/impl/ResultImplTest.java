package com.hazelcast.simulator.probes.probes.impl;

import com.hazelcast.simulator.probes.probes.Result;
import org.HdrHistogram.Histogram;
import org.junit.Test;

import static com.hazelcast.simulator.probes.probes.utils.ResultTestUtils.createProbeResult;
import static com.hazelcast.simulator.probes.probes.utils.ResultTestUtils.createRandomHistogram;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class ResultImplTest {

    @Test
    public void testConstructor() {
        Result result = new ResultImpl("ResultImplTest", 1000, 500.0);

        assertEquals("ResultImplTest", result.getTestName());
        assertEquals(1000, result.getInvocations());
        assertEquals(500, result.getThroughput(), 0.0001);
        assertTrue(result.isEmpty());
        assertTrue(result.probeNames().isEmpty());
    }

    @Test
    public void testAddHistogram() {
        Result result = createProbeResult(1);
        assertTrue(result.probeNames().contains("probe1"));

        Histogram expected = result.getHistogram("probe1").copy();
        assertNotNull(expected);

        Histogram histogram = createRandomHistogram(500);
        result.addHistogram("probe1", histogram);
        expected.add(histogram);

        assertEquals(expected, result.getHistogram("probe1"));
    }

    @Test
    public void testAddHistogram_null() {
        Result result = createProbeResult(1);
        Histogram expected = result.getHistogram("probe1").copy();
        assertNotNull(expected);

        result.addHistogram("probe1", null);

        assertEquals(expected, result.getHistogram("probe1"));
    }

    @Test
    public void testToHumanString() {
        Result result = createProbeResult(1);
        assertNotNull(result.toHumanString("probe1"));
    }
}
