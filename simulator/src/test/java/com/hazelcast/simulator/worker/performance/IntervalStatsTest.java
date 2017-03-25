package com.hazelcast.simulator.worker.performance;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class IntervalStatsTest {

    @Test
    public void testIsEmpty() {
        assertTrue(new IntervalStats().isEmpty());
    }

    @Test
    public void testAdd() {
        IntervalStats addState = new IntervalStats(100, 5.0, 10.0, 175.0d, 150, 200);

        addState.add(new IntervalStats(150, 6.0, 12.0, 90.0d, 80, 100));

        assertEquals(250, addState.getOperationCount());
        assertEquals(11.0, addState.getIntervalThroughput(), 0.00001);
        assertEquals(22.0, addState.getTotalThroughput(), 0.00001);
        assertEquals(150, addState.getLatency999Percentile());
        assertEquals(175.0d, addState.getLatencyAvg(), 0.00001);
        assertEquals(200, addState.getLatencyMax());
    }

    @Test
    public void testAdd_emptyState() {
        IntervalStats addState = new IntervalStats(100, 5.0, 10.0, 550.0d, 300, 800);

        addState.add(new IntervalStats());

        assertEquals(100, addState.getOperationCount());
        assertEquals(5.0, addState.getIntervalThroughput(), 0.00001);
        assertEquals(10.0, addState.getTotalThroughput(), 0.00001);
        assertEquals(300, addState.getLatency999Percentile());
        assertEquals(550.0d, addState.getLatencyAvg(), 0.00001);
        assertEquals(800, addState.getLatencyMax());
    }

    @Test
    public void testAdd_toEmptyState() {
        IntervalStats addState = new IntervalStats();

        addState.add(new IntervalStats(100, 5.0, 10.0, 450.0d, 400, 500));

        assertEquals(100, addState.getOperationCount());
        assertEquals(5.0, addState.getIntervalThroughput(), 0.00001);
        assertEquals(10.0, addState.getTotalThroughput(), 0.00001);
        assertEquals(400, addState.getLatency999Percentile());
        assertEquals(450.0d, addState.getLatencyAvg(), 0.00001);
        assertEquals(500, addState.getLatencyMax());
    }

    @Test
    public void testAdd_withoutAddOperationCountAndThroughput() {
        IntervalStats addState = new IntervalStats(100, 5.0, 10.0, 175.0d, 150, 200);

        addState.add(new IntervalStats(150, 6.0, 12.0, 90.0d, 80, 100), false);

        assertEquals(150, addState.getOperationCount());
        assertEquals(6.0, addState.getIntervalThroughput(), 0.00001);
        assertEquals(12.0, addState.getTotalThroughput(), 0.00001);
        assertEquals(150, addState.getLatency999Percentile());
        assertEquals(175.0d, addState.getLatencyAvg(), 0.00001);
        assertEquals(200, addState.getLatencyMax());
    }

    @Test
    public void testAdd_withoutAddOperationCountAndThroughput_emptyState() {
        IntervalStats addState = new IntervalStats(100, 5.0, 10.0, 550.0d, 300, 800);

        addState.add(new IntervalStats(), false);

        assertEquals(100, addState.getOperationCount());
        assertEquals(5.0, addState.getIntervalThroughput(), 0.00001);
        assertEquals(10.0, addState.getTotalThroughput(), 0.00001);
        assertEquals(300, addState.getLatency999Percentile());
        assertEquals(550.0d, addState.getLatencyAvg(), 0.00001);
        assertEquals(800, addState.getLatencyMax());
    }

    @Test
    public void testAdd_withoutAddOperationCountAndThroughput_toEmptyState() {
        IntervalStats addState = new IntervalStats();

        addState.add(new IntervalStats(100, 5.0, 10.0, 450.0d, 400, 500), false);

        assertEquals(100, addState.getOperationCount());
        assertEquals(5.0, addState.getIntervalThroughput(), 0.00001);
        assertEquals(10.0, addState.getTotalThroughput(), 0.00001);
        assertEquals(400, addState.getLatency999Percentile());
        assertEquals(450.0d, addState.getLatencyAvg(), 0.00001);
        assertEquals(500, addState.getLatencyMax());
    }

    @Test
    public void testToString() {
        assertNotNull(new IntervalStats().toString());
    }
}
