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

    private SimulatorAddress agentAddress1;
    private SimulatorAddress agentAddress2;

    @Before
    public void setUp() {
        emptyPerformanceStateContainer = new PerformanceStateContainer();
        performanceStateContainer = new PerformanceStateContainer();

        SimulatorAddress worker1 = new SimulatorAddress(AddressLevel.WORKER, 1, 1, 0);
        SimulatorAddress worker2 = new SimulatorAddress(AddressLevel.WORKER, 2, 1, 0);

        Map<String, PerformanceState> performanceStates1 = new HashMap<String, PerformanceState>();
        performanceStates1.put(TEST_CASE_ID_1, new PerformanceState(1000, 200, 500, 1900.0d, 1800, 2500));
        performanceStates1.put(TEST_CASE_ID_2, new PerformanceState(1500, 900, 800, 2300.0d, 2000, 2700));

        Map<String, PerformanceState> performanceStates2 = new HashMap<String, PerformanceState>();
        performanceStates2.put(TEST_CASE_ID_1, new PerformanceState(800, 100, 300, 2200.0d, 2400, 2800));
        performanceStates2.put(TEST_CASE_ID_2, new PerformanceState(1200, 700, 600, 2700.0d, 2600, 2900));

        performanceStateContainer.updatePerformanceState(worker1, performanceStates1);
        performanceStateContainer.updatePerformanceState(worker2, performanceStates2);

        agentAddress1 = worker1.getParent();
        agentAddress2 = worker2.getParent();
    }

    @After
    public void tearDown() {
        deleteQuiet(PERFORMANCE_FILE);
    }

    @Test
    public void testGetPerformanceNumbers() {
        String performance = performanceStateContainer.getPerformanceNumbers(TEST_CASE_ID_1);
        assertTrue(performance.contains("ops"));
    }

    @Test
    public void testGetPerformanceNumbers_testCaseNotFound() {
        String performance = performanceStateContainer.getPerformanceNumbers("notFound");
        assertFalse(performance.contains("ops"));
    }

    @Test
    public void testGetPerformanceNumbers_onEmptyContainer() {
        String performance = emptyPerformanceStateContainer.getPerformanceNumbers(TEST_CASE_ID_1);
        assertFalse(performance.contains("ops"));
    }

    @Test
    public void testGetPerformanceStateForTestCase() {
        PerformanceState performanceState = performanceStateContainer.getPerformanceStateForTestCase(TEST_CASE_ID_1);
        assertFalse(performanceState.isEmpty());
        assertEquals(1800, performanceState.getOperationCount());
        assertEquals(300, performanceState.getIntervalThroughput(), ASSERT_EQUALS_DELTA);
        assertEquals(800, performanceState.getTotalThroughput(), ASSERT_EQUALS_DELTA);
        assertEquals(2400, performanceState.getIntervalPercentileLatency());
        assertEquals(2200.0d, performanceState.getIntervalAvgLatency(), 0.001);
        assertEquals(2800, performanceState.getIntervalMaxLatency());
    }

    @Test
    public void testGetPerformanceStateForTestCase_testCaseNotFound() {
        PerformanceState performanceState = performanceStateContainer.getPerformanceStateForTestCase("notFound");
        assertTrue(performanceState.isEmpty());
    }

    @Test
    public void testGetPerformanceStateForTestCase_onEmptyContainer() {
        PerformanceState performanceState = emptyPerformanceStateContainer.getPerformanceStateForTestCase(TEST_CASE_ID_1);
        assertTrue(performanceState.isEmpty());
    }

    @Test
    public void testLogDetailedPerformanceInfo() {
        performanceStateContainer.logDetailedPerformanceInfo();

        String performance = fileAsText(PERFORMANCE_FILE);
        assertEquals("4500" + FormatUtils.NEW_LINE, performance);
    }

    @Test
    public void testLogDetailedPerformanceInfo_onEmptyContainer() {
        emptyPerformanceStateContainer.logDetailedPerformanceInfo();

        assertFalse(PERFORMANCE_FILE.exists());
    }

    @Test
    public void testCalculatePerformanceStates() {
        PerformanceState totalPerformanceState = new PerformanceState();
        Map<SimulatorAddress, PerformanceState> agentPerformanceStateMap = new HashMap<SimulatorAddress, PerformanceState>();

        performanceStateContainer.calculatePerformanceStates(totalPerformanceState, agentPerformanceStateMap);
        assertEquals(2, agentPerformanceStateMap.size());

        PerformanceState performanceStateAgent1 = agentPerformanceStateMap.get(agentAddress1);
        assertEquals(2500, performanceStateAgent1.getOperationCount());
        assertEquals(1100, performanceStateAgent1.getIntervalThroughput(), ASSERT_EQUALS_DELTA);
        assertEquals(1300, performanceStateAgent1.getTotalThroughput(), ASSERT_EQUALS_DELTA);
        assertEquals(2000, performanceStateAgent1.getIntervalPercentileLatency());
        assertEquals(2700, performanceStateAgent1.getIntervalMaxLatency());

        PerformanceState performanceStateAgent2 = agentPerformanceStateMap.get(agentAddress2);
        assertEquals(2000, performanceStateAgent2.getOperationCount());
        assertEquals(800, performanceStateAgent2.getIntervalThroughput(), ASSERT_EQUALS_DELTA);
        assertEquals(900, performanceStateAgent2.getTotalThroughput(), ASSERT_EQUALS_DELTA);
        assertEquals(2600, performanceStateAgent2.getIntervalPercentileLatency());
        assertEquals(2900, performanceStateAgent2.getIntervalMaxLatency());

        assertFalse(totalPerformanceState.isEmpty());
        assertEquals(4500, totalPerformanceState.getOperationCount());
        assertEquals(1900, totalPerformanceState.getIntervalThroughput(), ASSERT_EQUALS_DELTA);
        assertEquals(2200, totalPerformanceState.getTotalThroughput(), ASSERT_EQUALS_DELTA);
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
