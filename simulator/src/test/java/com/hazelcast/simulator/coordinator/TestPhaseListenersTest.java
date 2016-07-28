package com.hazelcast.simulator.coordinator;

import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.test.TestPhase;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestPhaseListenersTest {

    private TestPhaseListeners testPhaseListeners;

    @Before
    public void setUp() {
        testPhaseListeners = new TestPhaseListeners();
    }

    @Test
    public void testGetListeners() {
        assertTrue(testPhaseListeners.getListeners().isEmpty());
    }

    @Test
    public void testAddListener() {
        UnitTestPhaseListener listener = new UnitTestPhaseListener();

        testPhaseListeners.addListener(1, listener);

        assertTrue(testPhaseListeners.getListeners().contains(listener));
    }

    @Test
    public void testUpdatePhaseCompletion() {
        UnitTestPhaseListener listener = new UnitTestPhaseListener();
        testPhaseListeners.addListener(1, listener);

        testPhaseListeners.updatePhaseCompletion(1, TestPhase.GLOBAL_WARMUP, null);

        assertEquals(TestPhase.GLOBAL_WARMUP, listener.lastTestPhase);
    }

    @Test
    public void testUpdatePhaseCompletion_listenerNotFound() {
        UnitTestPhaseListener listener = new UnitTestPhaseListener();
        testPhaseListeners.addListener(1, listener);

        testPhaseListeners.updatePhaseCompletion(2, TestPhase.GLOBAL_WARMUP, null);

        assertEquals(null, listener.lastTestPhase);
    }

    private static class UnitTestPhaseListener implements TestPhaseListener {

        private TestPhase lastTestPhase;

        @Override
        public void completed(TestPhase testPhase, SimulatorAddress workerAddress) {
            lastTestPhase = testPhase;
        }
    }
}
