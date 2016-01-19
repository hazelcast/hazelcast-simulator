package com.hazelcast.simulator.test;

import org.junit.Test;

import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static com.hazelcast.simulator.test.TestPhase.getTestPhaseSyncMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TestPhaseTest {

    @Test
    public void testGetTestPhaseSyncMap() {
        Map<TestPhase, CountDownLatch> testPhaseSyncMap = getTestPhaseSyncMap(5, true, TestPhase.RUN);

        assertEquals(5, testPhaseSyncMap.get(TestPhase.SETUP).getCount());
        assertEquals(5, testPhaseSyncMap.get(TestPhase.LOCAL_WARMUP).getCount());
        assertEquals(5, testPhaseSyncMap.get(TestPhase.GLOBAL_WARMUP).getCount());
        assertEquals(5, testPhaseSyncMap.get(TestPhase.RUN).getCount());
        assertEquals(0, testPhaseSyncMap.get(TestPhase.GLOBAL_VERIFY).getCount());
        assertEquals(0, testPhaseSyncMap.get(TestPhase.LOCAL_VERIFY).getCount());
        assertEquals(0, testPhaseSyncMap.get(TestPhase.GLOBAL_TEARDOWN).getCount());
        assertEquals(0, testPhaseSyncMap.get(TestPhase.LOCAL_TEARDOWN).getCount());
    }

    @Test
    @SuppressWarnings("all")
    public void testGetTestPhaseSyncMap_notParallel() {
        Map<TestPhase, CountDownLatch> testPhaseSyncMap = getTestPhaseSyncMap(5, false, TestPhase.RUN);

        assertNull(testPhaseSyncMap);
    }
}
