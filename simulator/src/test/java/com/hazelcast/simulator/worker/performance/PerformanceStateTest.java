package com.hazelcast.simulator.worker.performance;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class PerformanceStateTest {

    @Test
    public void testIsEmpty() {
        assertTrue(new PerformanceState().isEmpty());
    }

    @Test
    public void testAdd() {
        PerformanceState addState = new PerformanceState(100, 5.0, 10.0, 150, 200);

        addState.add(new PerformanceState(150, 6.0, 12.0, 80, 100));

        assertEquals(250, addState.getOperationCount());
        assertEquals(11.0, addState.getIntervalThroughput(), 0.00001);
        assertEquals(22.0, addState.getTotalThroughput(), 0.00001);
        assertEquals(150, addState.getIntervalPercentileLatency());
        assertEquals(200, addState.getIntervalMaxLatency());
    }

    @Test
    public void testAdd_emptyState() {
        PerformanceState addState = new PerformanceState(100, 5.0, 10.0, 300, 800);

        addState.add(new PerformanceState());

        assertEquals(100, addState.getOperationCount());
        assertEquals(5.0, addState.getIntervalThroughput(), 0.00001);
        assertEquals(10.0, addState.getTotalThroughput(), 0.00001);
        assertEquals(300, addState.getIntervalPercentileLatency());
        assertEquals(800, addState.getIntervalMaxLatency());
    }

    @Test
    public void testAdd_toEmptyState() {
        PerformanceState addState = new PerformanceState();

        addState.add(new PerformanceState(100, 5.0, 10.0, 400, 500));

        assertEquals(100, addState.getOperationCount());
        assertEquals(5.0, addState.getIntervalThroughput(), 0.00001);
        assertEquals(10.0, addState.getTotalThroughput(), 0.00001);
        assertEquals(400, addState.getIntervalPercentileLatency());
        assertEquals(500, addState.getIntervalMaxLatency());
    }

    @Test
    public void testToString() {
        assertNotNull(new PerformanceState().toString());
    }
}
