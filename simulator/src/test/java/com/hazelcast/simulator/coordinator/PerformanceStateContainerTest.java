package com.hazelcast.simulator.coordinator;

import com.hazelcast.simulator.protocol.core.AddressLevel;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.utils.FormatUtils;
import com.hazelcast.simulator.worker.performance.PerformanceState;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.simulator.utils.FileUtils.deleteQuiet;
import static com.hazelcast.simulator.utils.FileUtils.fileAsText;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class PerformanceStateContainerTest {

    private static final double ASSERT_EQUALS_DELTA = 0.1;

    private static final String TEST_CASE_ID_1 = "testCase1";
    private static final String TEST_CASE_ID_2 = "testCase2";

    private static final File PERFORMANCE_FILE = new File(PerformanceStateContainer.PERFORMANCE_FILE_NAME);

    private PerformanceStateContainer emptyPerformanceStateContainer;
    private PerformanceStateContainer performanceStateContainer;

    private SimulatorAddress worker1;
    private SimulatorAddress worker2;

    private SimulatorAddress agentAddress1;
    private SimulatorAddress agentAddress2;

    @Before
    public void setUp() {
        emptyPerformanceStateContainer = new PerformanceStateContainer();
        performanceStateContainer = new PerformanceStateContainer();

        worker1 = new SimulatorAddress(AddressLevel.WORKER, 1, 1, 0);
        worker2 = new SimulatorAddress(AddressLevel.WORKER, 2, 1, 0);

        agentAddress1 = worker1.getParent();
        agentAddress2 = worker2.getParent();
    }

    @After
    public void tearDown() {
        deleteQuiet(PERFORMANCE_FILE);
    }

    @Test
    public void testFormatPerformanceNumbers() {
        update(worker1, TEST_CASE_ID_1, new PerformanceState(1000, 200, 500, 1900.0d, 1800, 2500));

        String performance = performanceStateContainer.formatPerformanceNumbers(TEST_CASE_ID_1);
        assertTrue(performance.contains("ops"));
    }

    @Test
    public void testFormatPerformanceNumbers_testCaseNotFound() {
        String performance = performanceStateContainer.formatPerformanceNumbers("notFound");
        assertFalse(performance.contains("ops"));
    }

    @Test
    public void testFormatPerformanceNumbers_onEmptyContainer() {
        String performance = emptyPerformanceStateContainer.formatPerformanceNumbers(TEST_CASE_ID_1);
        assertFalse(performance.contains("ops"));
    }

    @Test
    public void testFormatPerformanceNumbers_avgLatencyOverMicrosThreshold() throws Exception {
        SimulatorAddress worker = new SimulatorAddress(AddressLevel.WORKER, 3, 1, 0);

        Map<String, PerformanceState> performanceStates = new HashMap<String, PerformanceState>();
        performanceStates.put(TEST_CASE_ID_1, new PerformanceState(800, 100, 300, TimeUnit.SECONDS.toMicros(3), 2400, 2500));

        performanceStateContainer.update(worker, performanceStates);

        String performance = performanceStateContainer.formatPerformanceNumbers(TEST_CASE_ID_1);
        assertTrue(performance.contains("ms"));
        assertFalse(performance.contains("µs"));
    }

    private void update(SimulatorAddress address, String testId, PerformanceState performanceState) {
        Map<String, PerformanceState> performanceStateMap = new HashMap<String, PerformanceState>();
        performanceStateMap.put(testId, performanceState);
        performanceStateContainer.update(address, performanceStateMap);
    }

    @Test
    public void testGet() {
        update(worker1, TEST_CASE_ID_1, new PerformanceState(1000, 200, 500, 1900.0d, 1800, 2500));
        update(worker1, TEST_CASE_ID_1, new PerformanceState(1500, 150, 550, 1600.0d, 1700, 2400));
        update(worker2, TEST_CASE_ID_1, new PerformanceState(800, 100, 300, 2200.0d, 2400, 2800));

        PerformanceState performanceState = performanceStateContainer.get(TEST_CASE_ID_1);

        assertFalse(performanceState.isEmpty());
        assertEquals(2300, performanceState.getOperationCount());
        assertEquals(300.0, performanceState.getIntervalThroughput(), ASSERT_EQUALS_DELTA);
        assertEquals(850.0, performanceState.getTotalThroughput(), ASSERT_EQUALS_DELTA);
        assertEquals(2400, performanceState.getIntervalPercentileLatency());
        assertEquals(2200.0d, performanceState.getIntervalAvgLatency(), 0.001);
        assertEquals(2800, performanceState.getIntervalMaxLatency());
    }

    @Test
    public void testGet_testCaseNotFound() {
        PerformanceState performanceState = performanceStateContainer.get("notFound");

        assertTrue(performanceState.isEmpty());
    }

    @Test
    public void testGet_onEmptyContainer() {
        PerformanceState performanceState = emptyPerformanceStateContainer.get(TEST_CASE_ID_1);

        assertTrue(performanceState.isEmpty());
    }

    @Test
    public void testLogDetailedPerformanceInfo() {
        update(worker1, TEST_CASE_ID_1, new PerformanceState(1000, 200, 500, 1900.0d, 1800, 2500));

        performanceStateContainer.logDetailedPerformanceInfo(1);

        String performance = fileAsText(PERFORMANCE_FILE);
        assertEquals("1000" + FormatUtils.NEW_LINE, performance);
    }

    @Test
    public void testLogDetailedPerformanceInfo_onEmptyContainer() {
        emptyPerformanceStateContainer.logDetailedPerformanceInfo(1);

        assertFalse(PERFORMANCE_FILE.exists());
    }

    @Test
    public void testCalculatePerformanceStates() {
        update(worker1, TEST_CASE_ID_1, new PerformanceState(1000, 200, 500, 1900.0d, 1800, 2500));
        update(worker1, TEST_CASE_ID_2, new PerformanceState(1500, 900, 800, 2300.0d, 2000, 2700));

        update(worker1, TEST_CASE_ID_1, new PerformanceState(1500, 150, 550, 1600.0d, 1700, 2400));
        update(worker1, TEST_CASE_ID_2, new PerformanceState(2000, 950, 850, 2400.0d, 2100, 2800));

        update(worker2, TEST_CASE_ID_1, new PerformanceState(800, 100, 300, 2200.0d, 2400, 2800));
        update(worker2, TEST_CASE_ID_2,  new PerformanceState(1200, 700, 600, 2700.0d, 2600, 2900));

        PerformanceState totalPerformanceState = new PerformanceState();
        Map<SimulatorAddress, PerformanceState> agentPerformanceStateMap = new HashMap<SimulatorAddress, PerformanceState>();

        performanceStateContainer.calculatePerformanceStates(totalPerformanceState, agentPerformanceStateMap);

        assertEquals(2, agentPerformanceStateMap.size());

        PerformanceState performanceStateAgent1 = agentPerformanceStateMap.get(agentAddress1);
        assertEquals(3500, performanceStateAgent1.getOperationCount());
        assertEquals(1100, performanceStateAgent1.getIntervalThroughput(), ASSERT_EQUALS_DELTA);
        assertEquals(1400, performanceStateAgent1.getTotalThroughput(), ASSERT_EQUALS_DELTA);
        assertEquals(2100, performanceStateAgent1.getIntervalPercentileLatency());
        assertEquals(2800, performanceStateAgent1.getIntervalMaxLatency());

        PerformanceState performanceStateAgent2 = agentPerformanceStateMap.get(agentAddress2);
        assertEquals(2000, performanceStateAgent2.getOperationCount());
        assertEquals(800, performanceStateAgent2.getIntervalThroughput(), ASSERT_EQUALS_DELTA);
        assertEquals(900, performanceStateAgent2.getTotalThroughput(), ASSERT_EQUALS_DELTA);
        assertEquals(2600, performanceStateAgent2.getIntervalPercentileLatency());
        assertEquals(2900, performanceStateAgent2.getIntervalMaxLatency());

        assertFalse(totalPerformanceState.isEmpty());
        assertEquals(5500, totalPerformanceState.getOperationCount());
        assertEquals(1900, totalPerformanceState.getIntervalThroughput(), ASSERT_EQUALS_DELTA);
        assertEquals(2300, totalPerformanceState.getTotalThroughput(), ASSERT_EQUALS_DELTA);
        assertEquals(2600, totalPerformanceState.getIntervalPercentileLatency());
        assertEquals(2900, totalPerformanceState.getIntervalMaxLatency());
    }

    @Test
    public void testCalculatePerformanceStates_onEmptyContainer() {
        PerformanceState totalPerformanceState = new PerformanceState();
        Map<SimulatorAddress, PerformanceState> agentPerformanceStateMap = new HashMap<SimulatorAddress, PerformanceState>();

        emptyPerformanceStateContainer.calculatePerformanceStates(totalPerformanceState, agentPerformanceStateMap);

        assertEquals(0, agentPerformanceStateMap.size());
        assertTrue(totalPerformanceState.isEmpty());
    }
}
