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
package com.hazelcast.simulator.worker.testcontainer;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.simulator.test.annotations.InjectHazelcastInstance;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class TestContainer_InjectHazelcastInstanceTest extends TestContainer_AbstractTest {

    @Test
    public void testInjectHazelcastInstance() {
        HazelcastInstanceTest test = new HazelcastInstanceTest();
        testContainer = createTestContainer(test);

        assertNotNull(test.hazelcastInstance);
        assertEquals(testContext.getTargetInstance(), test.hazelcastInstance);
    }

    @Test
    public void testInjectHazelcastInstance_withoutAnnotation() {
        HazelcastInstanceTest test = new HazelcastInstanceTest();
        testContainer = createTestContainer(test);

        assertNull(test.notAnnotatedHazelcastInstance);
    }

    @SuppressWarnings("unused")
    private static class HazelcastInstanceTest extends BaseTest {

        @InjectHazelcastInstance
        private HazelcastInstance hazelcastInstance;

        private HazelcastInstance notAnnotatedHazelcastInstance;
    }

    @Test(expected = IllegalTestException.class)
    public void testInjectHazelcastInstance_withIllegalFieldType() {
        IllegalFieldTypeTest test = new IllegalFieldTypeTest();
        testContainer = createTestContainer(test);
    }

    @SuppressWarnings("unused")
    private static class IllegalFieldTypeTest extends BaseTest {

        @InjectHazelcastInstance
        private Object noProbeField;
    }
}
