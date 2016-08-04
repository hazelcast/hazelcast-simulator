package com.hazelcast.simulator.worker.performance;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class PerformanceStatsTest {

    @Test
    public void testIsEmpty() {
        assertTrue(new PerformanceStats().isEmpty());
    }

    @Test
    public void testAdd() {
        PerformanceStats addState = new PerformanceStats(100, 5.0, 10.0, 175.0d, 150, 200);

        addState.add(new PerformanceStats(150, 6.0, 12.0, 90.0d, 80, 100));

        assertEquals(250, addState.getOperationCount());
        assertEquals(11.0, addState.getIntervalThroughput(), 0.00001);
        assertEquals(22.0, addState.getTotalThroughput(), 0.00001);
        assertEquals(150, addState.getIntervalLatency999PercentileNanos());
        assertEquals(175.0d, addState.getIntervalLatencyAvgNanos(), 0.00001);
        assertEquals(200, addState.getIntervalLatencyMaxNanos());
    }

    @Test
    public void testAdd_emptyState() {
        PerformanceStats addState = new PerformanceStats(100, 5.0, 10.0, 550.0d, 300, 800);

        addState.add(new PerformanceStats());

        assertEquals(100, addState.getOperationCount());
        assertEquals(5.0, addState.getIntervalThroughput(), 0.00001);
        assertEquals(10.0, addState.getTotalThroughput(), 0.00001);
        assertEquals(300, addState.getIntervalLatency999PercentileNanos());
        assertEquals(550.0d, addState.getIntervalLatencyAvgNanos(), 0.00001);
        assertEquals(800, addState.getIntervalLatencyMaxNanos());
    }

    @Test
    public void testAdd_toEmptyState() {
        PerformanceStats addState = new PerformanceStats();

        addState.add(new PerformanceStats(100, 5.0, 10.0, 450.0d, 400, 500));

        assertEquals(100, addState.getOperationCount());
        assertEquals(5.0, addState.getIntervalThroughput(), 0.00001);
        assertEquals(10.0, addState.getTotalThroughput(), 0.00001);
        assertEquals(400, addState.getIntervalLatency999PercentileNanos());
        assertEquals(450.0d, addState.getIntervalLatencyAvgNanos(), 0.00001);
        assertEquals(500, addState.getIntervalLatencyMaxNanos());
    }

    @Test
    public void testAdd_withoutAddOperationCountAndThroughput() {
        PerformanceStats addState = new PerformanceStats(100, 5.0, 10.0, 175.0d, 150, 200);

        addState.add(new PerformanceStats(150, 6.0, 12.0, 90.0d, 80, 100), false);

        assertEquals(150, addState.getOperationCount());
        assertEquals(6.0, addState.getIntervalThroughput(), 0.00001);
        assertEquals(12.0, addState.getTotalThroughput(), 0.00001);
        assertEquals(150, addState.getIntervalLatency999PercentileNanos());
        assertEquals(175.0d, addState.getIntervalLatencyAvgNanos(), 0.00001);
        assertEquals(200, addState.getIntervalLatencyMaxNanos());
    }

    @Test
    public void testAdd_withoutAddOperationCountAndThroughput_emptyState() {
        PerformanceStats addState = new PerformanceStats(100, 5.0, 10.0, 550.0d, 300, 800);

        addState.add(new PerformanceStats(), false);

        assertEquals(100, addState.getOperationCount());
        assertEquals(5.0, addState.getIntervalThroughput(), 0.00001);
        assertEquals(10.0, addState.getTotalThroughput(), 0.00001);
        assertEquals(300, addState.getIntervalLatency999PercentileNanos());
        assertEquals(550.0d, addState.getIntervalLatencyAvgNanos(), 0.00001);
        assertEquals(800, addState.getIntervalLatencyMaxNanos());
    }

    @Test
    public void testAdd_withoutAddOperationCountAndThroughput_toEmptyState() {
        PerformanceStats addState = new PerformanceStats();

        addState.add(new PerformanceStats(100, 5.0, 10.0, 450.0d, 400, 500), false);

        assertEquals(100, addState.getOperationCount());
        assertEquals(5.0, addState.getIntervalThroughput(), 0.00001);
        assertEquals(10.0, addState.getTotalThroughput(), 0.00001);
        assertEquals(400, addState.getIntervalLatency999PercentileNanos());
        assertEquals(450.0d, addState.getIntervalLatencyAvgNanos(), 0.00001);
        assertEquals(500, addState.getIntervalLatencyMaxNanos());
    }

    @Test
    public void testToString() {
        assertNotNull(new PerformanceStats().toString());
    }
}
