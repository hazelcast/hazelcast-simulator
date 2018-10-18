package com.hazelcast.simulator.coordinator;

import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.worker.performance.IntervalStats;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.simulator.protocol.core.SimulatorAddress.workerAddress;
import static com.hazelcast.simulator.worker.performance.IntervalStats.aggregateAll;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class IntervalStatsCollectorTest {

    private static final double ASSERT_EQUALS_DELTA = 0.1;

    private static final String TEST_CASE_ID_1 = "testCase1";
    private static final String TEST_CASE_ID_2 = "testCase2";

    private PerformanceStatsCollector emptyPerformanceStatsCollector;
    private PerformanceStatsCollector performanceStatsCollector;

    private SimulatorAddress a1w1;
    private SimulatorAddress a2w1;
    private SimulatorAddress a1w2;
    private SimulatorAddress a2w2;

    private SimulatorAddress a1;
    private SimulatorAddress a2;

    @Before
    public void before() {
        emptyPerformanceStatsCollector = new PerformanceStatsCollector();
        performanceStatsCollector = new PerformanceStatsCollector();

        a1w1 = workerAddress(1, 1);
        a1w2 = workerAddress(1, 2);
        a2w1 = workerAddress(2, 1);
        a2w2 = workerAddress(2, 2);

        a1 = a1w1.getParent();
        a2 = a2w1.getParent();
    }

    @Test
    public void testFormatPerformanceNumbers() {
        update(a1w1, TEST_CASE_ID_1, new IntervalStats(1000, 200, 500, 1900.0d, 1800, 2500));

        String performance = performanceStatsCollector.formatIntervalPerformanceNumbers(TEST_CASE_ID_1);
        assertTrue(performance.contains("ops"));
    }

    @Test
    public void testFormatPerformanceNumbers_testCaseNotFound() {
        String performance = performanceStatsCollector.formatIntervalPerformanceNumbers("notFound");
        assertFalse(performance.contains("ops"));
    }

    @Test
    public void testFormatPerformanceNumbers_onEmptyContainer() {
        String performance = emptyPerformanceStatsCollector.formatIntervalPerformanceNumbers(TEST_CASE_ID_1);
        assertFalse(performance.contains("ops"));
    }

    @Test
    public void testFormatPerformanceNumbers_avgLatencyOverMicrosThreshold() throws Exception {
        SimulatorAddress worker = workerAddress(3, 1);

        Map<String, IntervalStats> performanceStats = new HashMap<String, IntervalStats>();
        performanceStats.put(TEST_CASE_ID_1, new IntervalStats(
                800, 100, 300, SECONDS.toNanos(3), MICROSECONDS.toNanos(2400), MICROSECONDS.toNanos(2500)));

        performanceStatsCollector.update(worker, performanceStats);

        String performance = performanceStatsCollector.formatIntervalPerformanceNumbers(TEST_CASE_ID_1);
        assertTrue(performance.contains("ms"));
        assertFalse(performance.contains("µs"));
    }

    private void update(SimulatorAddress address, String testId, IntervalStats intervalStats) {
        Map<String, IntervalStats> performanceStatsMap = new HashMap<String, IntervalStats>();
        performanceStatsMap.put(testId, intervalStats);
        performanceStatsCollector.update(address, performanceStatsMap);
    }

    @Test
    public void testGet() {
        update(a1w1, TEST_CASE_ID_1, new IntervalStats(1000, 200, 500, 1900.0d, 1800, 2500));
        update(a1w1, TEST_CASE_ID_1, new IntervalStats(1500, 150, 550, 1600.0d, 1700, 2400));
        update(a2w1, TEST_CASE_ID_1, new IntervalStats(800, 100, 300, 2200.0d, 2400, 2800));

        IntervalStats intervalStats = performanceStatsCollector.get(TEST_CASE_ID_1, true);

        assertFalse(intervalStats.isEmpty());
        assertEquals(2300, intervalStats.getOperationCount());
        assertEquals(300.0, intervalStats.getIntervalThroughput(), ASSERT_EQUALS_DELTA);
        assertEquals(850.0, intervalStats.getTotalThroughput(), ASSERT_EQUALS_DELTA);
        assertEquals(2400, intervalStats.getLatency999Percentile());
        assertEquals(2200.0d, intervalStats.getLatencyAvg(), 0.001);
        assertEquals(2800, intervalStats.getLatencyMax());
    }

    @Test
    public void testGet_testCaseNotFound() {
        IntervalStats intervalStats = performanceStatsCollector.get("notFound", true);

        assertTrue(intervalStats.isEmpty());
    }

    @Test
    public void testGet_onEmptyContainer() {
        IntervalStats intervalStats = emptyPerformanceStatsCollector.get(TEST_CASE_ID_1, true);

        assertTrue(intervalStats.isEmpty());
    }

    @Test
    public void testCalculatePerformanceStats() {
        IntervalStats a1w1Stats = new IntervalStats(100, 10, 100.0, 50, 100, 200);
        IntervalStats a1w2Stats = new IntervalStats(200, 20, 200.0, 60, 110, 210);

        update(a1w1, TEST_CASE_ID_1, a1w1Stats);
        update(a1w2, TEST_CASE_ID_1, a1w2Stats);

        IntervalStats a2w1Stats = new IntervalStats(300, 30, 300.0, 70, 120, 220);
        IntervalStats a2w2Stats = new IntervalStats(400, 40, 400.0, 80, 120, 230);

        update(a2w1, TEST_CASE_ID_1, a2w1Stats);
        update(a2w2, TEST_CASE_ID_1, a2w2Stats);

        IntervalStats totalStats = new IntervalStats();
        Map<SimulatorAddress, IntervalStats> agentStats = new HashMap<SimulatorAddress, IntervalStats>();

        performanceStatsCollector.calculatePerformanceStats(TEST_CASE_ID_1, totalStats, agentStats);

        assertEquals(2, agentStats.size());

        assertPerfStatEquals(aggregateAll(a1w1Stats, a1w2Stats), agentStats.get(a1));
        assertPerfStatEquals(aggregateAll(a2w1Stats, a2w2Stats), agentStats.get(a2));

        assertPerfStatEquals(aggregateAll(a1w1Stats, a1w2Stats, a2w1Stats, a2w2Stats), totalStats);
    }

    @Test
    public void testCalculatePerformanceStats_differentTests() {
        IntervalStats a1w1Stats = new IntervalStats(100, 10, 100.0, 50, 100, 200);
        IntervalStats a1w2Stats = new IntervalStats(200, 20, 200.0, 60, 110, 210);

        update(a1w1, TEST_CASE_ID_1, a1w1Stats);
        update(a1w2, TEST_CASE_ID_2, a1w2Stats);

        IntervalStats a2w1Stats = new IntervalStats(300, 30, 300.0, 70, 120, 220);
        IntervalStats a2w2Stats = new IntervalStats(400, 40, 400.0, 80, 120, 230);

        update(a2w1, TEST_CASE_ID_1, a2w1Stats);
        update(a2w2, TEST_CASE_ID_2, a2w2Stats);

        IntervalStats totalStats = new IntervalStats();
        Map<SimulatorAddress, IntervalStats> agentStats = new HashMap<SimulatorAddress, IntervalStats>();

        performanceStatsCollector.calculatePerformanceStats(TEST_CASE_ID_1, totalStats, agentStats);

        assertEquals(2, agentStats.size());

        assertPerfStatEquals(a1w1Stats, agentStats.get(a1));
        assertPerfStatEquals(a2w1Stats, agentStats.get(a2));

        assertPerfStatEquals(aggregateAll(a1w1Stats, a2w1Stats), totalStats);
    }

    private void assertPerfStatEquals(IntervalStats expected, IntervalStats actual) {
        assertEquals(expected.getOperationCount(), actual.getOperationCount());
        assertEquals(expected.getIntervalThroughput(), actual.getIntervalThroughput(), ASSERT_EQUALS_DELTA);
        assertEquals(actual.getTotalThroughput(), actual.getTotalThroughput(), ASSERT_EQUALS_DELTA);
        assertEquals(actual.getLatency999Percentile(), actual.getLatency999Percentile());
        assertEquals(actual.getLatencyMax(), actual.getLatencyMax());
    }

    @Test
    public void testCalculatePerformanceStats_onEmptyContainer() {
        IntervalStats totalIntervalStats = new IntervalStats();
        Map<SimulatorAddress, IntervalStats> agentPerformanceStatsMap = new HashMap<SimulatorAddress, IntervalStats>();

        emptyPerformanceStatsCollector.calculatePerformanceStats("foo", totalIntervalStats, agentPerformanceStatsMap);

        assertEquals(0, agentPerformanceStatsMap.size());
        assertTrue(totalIntervalStats.isEmpty());
    }
}
