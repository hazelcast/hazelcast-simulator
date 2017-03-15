package com.hazelcast.simulator.worker.testcontainer;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.simulator.test.annotations.InjectVendor;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;

public class TestContainer_InjectHazelcastInstanceTest extends TestContainer_AbstractTest {

    @Test
    public void testInjectHazelcastInstance() {
        Class list = Object.class;
        Class linkedList = mock(List.class).getClass();

        System.out.println(list.isAssignableFrom(linkedList));


        HazelcastInstance hazelcastInstance = mock(HazelcastInstance.class);
        HazelcastInstanceTest test = new HazelcastInstanceTest();
        testContainer = createTestContainer(test, hazelcastInstance);

        assertNotNull(test.hazelcastInstance);
        assertEquals(hazelcastInstance, test.hazelcastInstance);
    }

    @Test
    public void whenNoAnnotation() {
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
    public void whenWrongFieldType() {
        WrongFieldTypeTest test = new WrongFieldTypeTest();
        testContainer = createTestContainer(test);
    }

    private static class WrongFieldTypeTest extends BaseTest {

        @InjectVendor
        private String noProbeField;
    }
}
