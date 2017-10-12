/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.simulator.coordinator;

import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.worker.performance.PerformanceStats;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.simulator.protocol.core.AddressLevel.WORKER;
import static com.hazelcast.simulator.worker.performance.PerformanceStats.aggregateAll;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class PerformanceStatsCollectorTest {

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

        a1w1 = new SimulatorAddress(WORKER, 1, 1, 0);
        a1w2 = new SimulatorAddress(WORKER, 1, 2, 0);
        a2w1 = new SimulatorAddress(WORKER, 2, 1, 0);
        a2w2 = new SimulatorAddress(WORKER, 2, 2, 0);

        a1 = a1w1.getParent();
        a2 = a2w1.getParent();
    }

    @Test
    public void testFormatPerformanceNumbers() {
        update(a1w1, TEST_CASE_ID_1, new PerformanceStats(1000, 200, 500, 1900.0d, 1800, 2500));

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
    public void testFormatPerformanceNumbers_avgLatencyOverMicrosThreshold() {
        SimulatorAddress worker = new SimulatorAddress(WORKER, 3, 1, 0);

        Map<String, PerformanceStats> performanceStats = new HashMap<String, PerformanceStats>();
        performanceStats.put(TEST_CASE_ID_1, new PerformanceStats(
                800, 100, 300, SECONDS.toNanos(3), MICROSECONDS.toNanos(2400), MICROSECONDS.toNanos(2500)));

        performanceStatsCollector.update(worker, performanceStats);

        String performance = performanceStatsCollector.formatIntervalPerformanceNumbers(TEST_CASE_ID_1);
        assertTrue(performance.contains("ms"));
        assertFalse(performance.contains("µs"));
    }

    private void update(SimulatorAddress address, String testId, PerformanceStats performanceStats) {
        Map<String, PerformanceStats> performanceStatsMap = new HashMap<String, PerformanceStats>();
        performanceStatsMap.put(testId, performanceStats);
        performanceStatsCollector.update(address, performanceStatsMap);
    }

    @Test
    public void testGet() {
        update(a1w1, TEST_CASE_ID_1, new PerformanceStats(1000, 200, 500, 1900.0d, 1800, 2500));
        update(a1w1, TEST_CASE_ID_1, new PerformanceStats(1500, 150, 550, 1600.0d, 1700, 2400));
        update(a2w1, TEST_CASE_ID_1, new PerformanceStats(800, 100, 300, 2200.0d, 2400, 2800));

        PerformanceStats performanceStats = performanceStatsCollector.get(TEST_CASE_ID_1, true);

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
        PerformanceStats performanceStats = performanceStatsCollector.get("notFound", true);

        assertTrue(performanceStats.isEmpty());
    }

    @Test
    public void testGet_onEmptyContainer() {
        PerformanceStats performanceStats = emptyPerformanceStatsCollector.get(TEST_CASE_ID_1, true);

        assertTrue(performanceStats.isEmpty());
    }

    @Test
    public void testCalculatePerformanceStats() {
        PerformanceStats a1w1Stats = new PerformanceStats(100, 10, 100.0, 50, 100, 200);
        PerformanceStats a1w2Stats = new PerformanceStats(200, 20, 200.0, 60, 110, 210);

        update(a1w1, TEST_CASE_ID_1, a1w1Stats);
        update(a1w2, TEST_CASE_ID_1, a1w2Stats);

        PerformanceStats a2w1Stats = new PerformanceStats(300, 30, 300.0, 70, 120, 220);
        PerformanceStats a2w2Stats = new PerformanceStats(400, 40, 400.0, 80, 120, 230);

        update(a2w1, TEST_CASE_ID_1, a2w1Stats);
        update(a2w2, TEST_CASE_ID_1, a2w2Stats);

        PerformanceStats totalStats = new PerformanceStats();
        Map<SimulatorAddress, PerformanceStats> agentStats = new HashMap<SimulatorAddress, PerformanceStats>();

        performanceStatsCollector.calculatePerformanceStats(TEST_CASE_ID_1, totalStats, agentStats);

        assertEquals(2, agentStats.size());

        assertPerfStatEquals(aggregateAll(a1w1Stats, a1w2Stats), agentStats.get(a1));
        assertPerfStatEquals(aggregateAll(a2w1Stats, a2w2Stats), agentStats.get(a2));

        assertPerfStatEquals(aggregateAll(a1w1Stats, a1w2Stats, a2w1Stats, a2w2Stats), totalStats);
    }

    @Test
    public void testCalculatePerformanceStats_differentTests() {
        PerformanceStats a1w1Stats = new PerformanceStats(100, 10, 100.0, 50, 100, 200);
        PerformanceStats a1w2Stats = new PerformanceStats(200, 20, 200.0, 60, 110, 210);

        update(a1w1, TEST_CASE_ID_1, a1w1Stats);
        update(a1w2, TEST_CASE_ID_2, a1w2Stats);

        PerformanceStats a2w1Stats = new PerformanceStats(300, 30, 300.0, 70, 120, 220);
        PerformanceStats a2w2Stats = new PerformanceStats(400, 40, 400.0, 80, 120, 230);

        update(a2w1, TEST_CASE_ID_1, a2w1Stats);
        update(a2w2, TEST_CASE_ID_2, a2w2Stats);

        PerformanceStats totalStats = new PerformanceStats();
        Map<SimulatorAddress, PerformanceStats> agentStats = new HashMap<SimulatorAddress, PerformanceStats>();

        performanceStatsCollector.calculatePerformanceStats(TEST_CASE_ID_1, totalStats, agentStats);

        assertEquals(2, agentStats.size());

        assertPerfStatEquals(a1w1Stats, agentStats.get(a1));
        assertPerfStatEquals(a2w1Stats, agentStats.get(a2));

        assertPerfStatEquals(aggregateAll(a1w1Stats, a2w1Stats), totalStats);
    }

    private void assertPerfStatEquals(PerformanceStats expected, PerformanceStats actual) {
        assertEquals(expected.getOperationCount(), actual.getOperationCount());
        assertEquals(expected.getIntervalThroughput(), actual.getIntervalThroughput(), ASSERT_EQUALS_DELTA);
        assertEquals(actual.getTotalThroughput(), actual.getTotalThroughput(), ASSERT_EQUALS_DELTA);
        assertEquals(actual.getIntervalLatency999PercentileNanos(), actual.getIntervalLatency999PercentileNanos());
        assertEquals(actual.getIntervalLatencyMaxNanos(), actual.getIntervalLatencyMaxNanos());
    }

    @Test
    public void testCalculatePerformanceStats_onEmptyContainer() {
        PerformanceStats totalPerformanceStats = new PerformanceStats();
        Map<SimulatorAddress, PerformanceStats> agentPerformanceStatsMap = new HashMap<SimulatorAddress, PerformanceStats>();

        emptyPerformanceStatsCollector.calculatePerformanceStats("foo", totalPerformanceStats, agentPerformanceStatsMap);

        assertEquals(0, agentPerformanceStatsMap.size());
        assertTrue(totalPerformanceStats.isEmpty());
    }
}
