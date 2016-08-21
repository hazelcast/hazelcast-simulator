package com.hazelcast.simulator.coordinator;

import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.common.TestPhase;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

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
    public void removeAllListeners() {
        TestPhaseListener listener1 = mock(TestPhaseListener.class);
        TestPhaseListener listener2 = mock(TestPhaseListener.class);
        TestPhaseListener listener3 = mock(TestPhaseListener.class);
        testPhaseListeners.addListener(1, listener1);
        testPhaseListeners.addListener(1, listener2);
        testPhaseListeners.addListener(1, listener3);

        testPhaseListeners.removeAllListeners(asList(listener1, listener2));

        assertEquals(singleton(listener3), new HashSet<TestPhaseListener>(testPhaseListeners.getListeners()));
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

        testPhaseListeners.updatePhaseCompletion(1, TestPhase.GLOBAL_PREPARE, null);

        assertEquals(TestPhase.GLOBAL_PREPARE, listener.lastTestPhase);
    }

    @Test
    public void testUpdatePhaseCompletion_listenerNotFound() {
        UnitTestPhaseListener listener = new UnitTestPhaseListener();
        testPhaseListeners.addListener(1, listener);

        testPhaseListeners.updatePhaseCompletion(2, TestPhase.GLOBAL_PREPARE, null);

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
