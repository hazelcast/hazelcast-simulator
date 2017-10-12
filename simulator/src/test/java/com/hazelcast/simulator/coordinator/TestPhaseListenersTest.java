/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.simulator.common.TestPhase;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
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
    public void before() {
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

        testPhaseListeners.onCompletion(1, TestPhase.GLOBAL_PREPARE, null);

        assertEquals(TestPhase.GLOBAL_PREPARE, listener.lastTestPhase);
    }

    @Test
    public void testUpdatePhaseCompletion_listenerNotFound() {
        UnitTestPhaseListener listener = new UnitTestPhaseListener();
        testPhaseListeners.addListener(1, listener);

        testPhaseListeners.onCompletion(2, TestPhase.GLOBAL_PREPARE, null);

        assertEquals(null, listener.lastTestPhase);
    }

    private static class UnitTestPhaseListener implements TestPhaseListener {

        private TestPhase lastTestPhase;

        @Override
        public void onCompletion(TestPhase testPhase, SimulatorAddress workerAddress) {
            lastTestPhase = testPhase;
        }
    }
}
