package com.hazelcast.simulator.test;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.simulator.test.annotations.InjectHazelcastInstance;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class TestContainer_InjectHazelcastInstanceTest extends AbstractTestContainerTest {

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

    private static class HazelcastInstanceTest extends BaseTest {

        @InjectHazelcastInstance
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

        @InjectHazelcastInstance
        private Object noProbeField;
    }
}
