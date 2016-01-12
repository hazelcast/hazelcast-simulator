package com.hazelcast.simulator.coordinator;

import com.hazelcast.simulator.test.TestPhase;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestPhaseListenerContainerTest {

    private TestPhaseListenerContainer testPhaseListenerContainer;

    @Before
    public void setUp() {
        testPhaseListenerContainer = new TestPhaseListenerContainer();
    }

    @Test
    public void testGetListeners() {
        assertTrue(testPhaseListenerContainer.getListeners().isEmpty());
    }

    @Test
    public void testAddListener() {
        UnitTestPhaseListener listener = new UnitTestPhaseListener();

        testPhaseListenerContainer.addListener(1, listener);

        assertTrue(testPhaseListenerContainer.getListeners().contains(listener));
    }

    @Test
    public void testUpdatePhaseCompletion() {
        UnitTestPhaseListener listener = new UnitTestPhaseListener();
        testPhaseListenerContainer.addListener(1, listener);

        testPhaseListenerContainer.updatePhaseCompletion(1, TestPhase.GLOBAL_WARMUP);

        assertEquals(TestPhase.GLOBAL_WARMUP, listener.lastTestPhase);
    }

    @Test
    public void testUpdatePhaseCompletion_listenerNotFound() {
        UnitTestPhaseListener listener = new UnitTestPhaseListener();
        testPhaseListenerContainer.addListener(1, listener);

        testPhaseListenerContainer.updatePhaseCompletion(2, TestPhase.GLOBAL_WARMUP);

        assertEquals(null, listener.lastTestPhase);
    }

    private static class UnitTestPhaseListener implements TestPhaseListener {

        private TestPhase lastTestPhase;

        @Override
        public void completed(TestPhase testPhase) {
            lastTestPhase = testPhase;
        }
    }
}
