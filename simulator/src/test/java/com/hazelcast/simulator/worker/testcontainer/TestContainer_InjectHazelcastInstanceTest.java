package com.hazelcast.simulator.worker.testcontainer;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.simulator.test.annotations.InjectVendor;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;

public class TestContainer_InjectHazelcastInstanceTest extends TestContainer_AbstractTest {

    @Test
    public void testInjectHazelcastInstance() {
        HazelcastInstance hazelcastInstance = mock(HazelcastInstance.class);
        HazelcastInstanceTest test = new HazelcastInstanceTest();
        testContainer = createTestContainer(test, hazelcastInstance);

        assertNotNull(test.hazelcastInstance);
        assertEquals(hazelcastInstance, test.hazelcastInstance);
    }

    @Test
    public void testInjectHazelcastInstance_withoutAnnotation() {
        HazelcastInstanceTest test = new HazelcastInstanceTest();
        testContainer = createTestContainer(test);

        assertNull(test.notAnnotatedHazelcastInstance);
    }

    private static class HazelcastInstanceTest extends BaseTest {

        @InjectVendor
        private HazelcastInstance hazelcastInstance;

        @SuppressWarnings("unused")
        private HazelcastInstance notAnnotatedHazelcastInstance;
    }

    @Test(expected = IllegalTestException.class)
    public void testInjectHazelcastInstance_withIllegalFieldType() {
        IllegalFieldTypeTest test = new IllegalFieldTypeTest();
        testContainer = createTestContainer(test);
    }

    private static class IllegalFieldTypeTest extends BaseTest {

        @InjectVendor
        private Object noProbeField;
    }
}
