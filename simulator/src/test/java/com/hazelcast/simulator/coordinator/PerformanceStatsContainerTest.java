package com.hazelcast.simulator.coordinator;

import com.hazelcast.simulator.protocol.core.AddressLevel;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.worker.performance.PerformanceStats;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class PerformanceStatsContainerTest {

    private static final double ASSERT_EQUALS_DELTA = 0.1;

    private static final String TEST_CASE_ID_1 = "testCase1";
    private static final String TEST_CASE_ID_2 = "testCase2";

    private PerformanceStatsContainer emptyPerformanceStatsContainer;
    private PerformanceStatsContainer performanceStatsContainer;

    private SimulatorAddress worker1;
    private SimulatorAddress worker2;

    private SimulatorAddress agentAddress1;
    private SimulatorAddress agentAddress2;

    @Before
    public void setUp() {
        emptyPerformanceStatsContainer = new PerformanceStatsContainer();
        performanceStatsContainer = new PerformanceStatsContainer();

        worker1 = new SimulatorAddress(AddressLevel.WORKER, 1, 1, 0);
        worker2 = new SimulatorAddress(AddressLevel.WORKER, 2, 1, 0);

        agentAddress1 = worker1.getParent();
        agentAddress2 = worker2.getParent();
    }

    @Test
    public void testFormatPerformanceNumbers() {
        update(worker1, TEST_CASE_ID_1, new PerformanceStats(1000, 200, 500, 1900.0d, 1800, 2500));

        String performance = performanceStatsContainer.formatPerformanceNumbers(TEST_CASE_ID_1);
        assertTrue(performance.contains("ops"));
    }

    @Test
    public void testFormatPerformanceNumbers_testCaseNotFound() {
        String performance = performanceStatsContainer.formatPerformanceNumbers("notFound");
        assertFalse(performance.contains("ops"));
    }

    @Test
    public void testFormatPerformanceNumbers_onEmptyContainer() {
        String performance = emptyPerformanceStatsContainer.formatPerformanceNumbers(TEST_CASE_ID_1);
        assertFalse(performance.contains("ops"));
    }

    @Test
    public void testFormatPerformanceNumbers_avgLatencyOverMicrosThreshold() throws Exception {
        SimulatorAddress worker = new SimulatorAddress(AddressLevel.WORKER, 3, 1, 0);

        Map<String, PerformanceStats> performanceStats = new HashMap<String, PerformanceStats>();
        performanceStats.put(TEST_CASE_ID_1, new PerformanceStats(
                800, 100, 300, SECONDS.toNanos(3), MICROSECONDS.toNanos(2400), MICROSECONDS.toNanos(2500)));

        performanceStatsContainer.update(worker, performanceStats);

        String performance = performanceStatsContainer.formatPerformanceNumbers(TEST_CASE_ID_1);
        assertTrue(performance.contains("ms"));
        assertFalse(performance.contains("µs"));
    }

    private void update(SimulatorAddress address, String testId, PerformanceStats performanceStats) {
        Map<String, PerformanceStats> performanceStatsMap = new HashMap<String, PerformanceStats>();
        performanceStatsMap.put(testId, performanceStats);
        performanceStatsContainer.update(address, performanceStatsMap);
    }

    @Test
    public void testGet() {
        update(worker1, TEST_CASE_ID_1, new PerformanceStats(1000, 200, 500, 1900.0d, 1800, 2500));
        update(worker1, TEST_CASE_ID_1, new PerformanceStats(1500, 150, 550, 1600.0d, 1700, 2400));
        update(worker2, TEST_CASE_ID_1, new PerformanceStats(800, 100, 300, 2200.0d, 2400, 2800));

        PerformanceStats performanceStats = performanceStatsContainer.get(TEST_CASE_ID_1);

        assertFalse(performanceStats.isEmpty());
        assertEquals(2300, performanceStats.getOperationCount());
        assertEquals(300.0, performanceStats.getIntervalThroughput(), ASSERT_EQUALS_DELTA);
        assertEquals(850.0, performanceStats.getTotalThroughput(), ASSERT_EQUALS_DELTA);
        assertEquals(2400, performanceStats.getIntervalLatency999PercentileNanos());
        assertEquals(2200.0d, performanceStats.getIntervalLatencyAvgNanos(), 0.001);
        assertEquals(2800, performanceStats.getIntervalLatencyMaxNanos());
    }

    @Test
    public void testGet_testCaseNotFound() {
        PerformanceStats performanceStats = performanceStatsContainer.get("notFound");

        assertTrue(performanceStats.isEmpty());
    }

    @Test
    public void testGet_onEmptyContainer() {
        PerformanceStats performanceStats = emptyPerformanceStatsContainer.get(TEST_CASE_ID_1);

        assertTrue(performanceStats.isEmpty());
    }

    @Test
    public void testCalculatePerformanceStats() {
        update(worker1, TEST_CASE_ID_1, new PerformanceStats(1000, 200, 500, 1900.0d, 1800, 2500));
        update(worker1, TEST_CASE_ID_2, new PerformanceStats(1500, 900, 800, 2300.0d, 2000, 2700));

        update(worker1, TEST_CASE_ID_1, new PerformanceStats(1500, 150, 550, 1600.0d, 1700, 2400));
        update(worker1, TEST_CASE_ID_2, new PerformanceStats(2000, 950, 850, 2400.0d, 2100, 2800));

        update(worker2, TEST_CASE_ID_1, new PerformanceStats(800, 100, 300, 2200.0d, 2400, 2800));
        update(worker2, TEST_CASE_ID_2,  new PerformanceStats(1200, 700, 600, 2700.0d, 2600, 2900));

        PerformanceStats totalPerformanceStats = new PerformanceStats();
        Map<SimulatorAddress, PerformanceStats> agentPerformanceStatsMap = new HashMap<SimulatorAddress, PerformanceStats>();

        performanceStatsContainer.calculatePerformanceStats(totalPerformanceStats, agentPerformanceStatsMap);

        assertEquals(2, agentPerformanceStatsMap.size());

        PerformanceStats performanceStatsAgent1 = agentPerformanceStatsMap.get(agentAddress1);
        assertEquals(3500, performanceStatsAgent1.getOperationCount());
        assertEquals(1100, performanceStatsAgent1.getIntervalThroughput(), ASSERT_EQUALS_DELTA);
        assertEquals(1400, performanceStatsAgent1.getTotalThroughput(), ASSERT_EQUALS_DELTA);
        assertEquals(2100, performanceStatsAgent1.getIntervalLatency999PercentileNanos());
        assertEquals(2800, performanceStatsAgent1.getIntervalLatencyMaxNanos());

        PerformanceStats performanceStatsAgent2 = agentPerformanceStatsMap.get(agentAddress2);
        assertEquals(2000, performanceStatsAgent2.getOperationCount());
        assertEquals(800, performanceStatsAgent2.getIntervalThroughput(), ASSERT_EQUALS_DELTA);
        assertEquals(900, performanceStatsAgent2.getTotalThroughput(), ASSERT_EQUALS_DELTA);
        assertEquals(2600, performanceStatsAgent2.getIntervalLatency999PercentileNanos());
        assertEquals(2900, performanceStatsAgent2.getIntervalLatencyMaxNanos());

        assertFalse(totalPerformanceStats.isEmpty());
        assertEquals(5500, totalPerformanceStats.getOperationCount());
        assertEquals(1900, totalPerformanceStats.getIntervalThroughput(), ASSERT_EQUALS_DELTA);
        assertEquals(2300, totalPerformanceStats.getTotalThroughput(), ASSERT_EQUALS_DELTA);
        assertEquals(2600, totalPerformanceStats.getIntervalLatency999PercentileNanos());
        assertEquals(2900, totalPerformanceStats.getIntervalLatencyMaxNanos());
    }

    @Test
    public void testCalculatePerformanceStats_onEmptyContainer() {
        PerformanceStats totalPerformanceStats = new PerformanceStats();
        Map<SimulatorAddress, PerformanceStats> agentPerformanceStatsMap = new HashMap<SimulatorAddress, PerformanceStats>();

        emptyPerformanceStatsContainer.calculatePerformanceStats(totalPerformanceStats, agentPerformanceStatsMap);

        assertEquals(0, agentPerformanceStatsMap.size());
        assertTrue(totalPerformanceStats.isEmpty());
    }
}
