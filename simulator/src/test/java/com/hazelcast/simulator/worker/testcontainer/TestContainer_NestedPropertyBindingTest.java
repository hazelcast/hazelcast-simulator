package com.hazelcast.simulator.worker.testcontainer;

import com.hazelcast.simulator.common.TestCase;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.utils.BindException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TestContainer_NestedPropertyBindingTest extends TestContainer_AbstractTest {

    @Test
    public void testNestProperties() {
        TestCase testCase = new TestCase("id")
                .setProperty("nested.intField", 10)
                .setProperty("nested.booleanField", true)
                .setProperty("nested.stringField", "somestring");

        DummyTest test = new DummyTest();

        testContainer = createTestContainer(test, testCase);

        assertNotNull(test.nested);
        assertEquals(10, test.nested.intField);
        assertEquals(true, test.nested.booleanField);
        assertEquals("somestring", test.nested.stringField);
    }

    @Test(expected = BindException.class)
    public void testNestedPropertyNotFound() {
        TestCase testCase = new TestCase("id")
                .setProperty("nested.notexist", 10);

        DummyTest test = new DummyTest();
        createTestContainer(test, testCase);
    }

    public class DummyTest {

        public NestedProperties nested = new NestedProperties();

        @TimeStep
        public void timeStep() {
        }
    }

    public class NestedProperties {

        public int intField;
        public boolean booleanField;
        public String stringField;
    }
}
