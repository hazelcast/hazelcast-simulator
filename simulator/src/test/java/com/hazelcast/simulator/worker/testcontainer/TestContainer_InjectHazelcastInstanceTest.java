package com.hazelcast.simulator.worker.testcontainer;

import com.hazelcast.simulator.test.annotations.InjectVendor;
import com.hazelcast.simulator.fake.FakeInstance;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;

public class TestContainer_InjectHazelcastInstanceTest extends TestContainer_AbstractTest {

    @Test
    public void testInjectVendorInstance() {
        Class list = Object.class;
        Class linkedList = mock(List.class).getClass();

        System.out.println(list.isAssignableFrom(linkedList));


        FakeInstance fakeVendorInstance = mock(FakeInstance.class);
        InstanceTest test = new InstanceTest();
        testContainer = createTestContainer(test, fakeVendorInstance);

        assertNotNull(test.instance);
        assertEquals(fakeVendorInstance, test.instance);
    }

    @Test
    public void whenNoAnnotation() {
        InstanceTest test = new InstanceTest();
        testContainer = createTestContainer(test);

        assertNull(test.notAnnotatedInstance);
    }

    private static class InstanceTest extends BaseTest {

        @InjectVendor
        private FakeInstance instance;

        @SuppressWarnings("unused")
        private FakeInstance notAnnotatedInstance;
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
