package com.hazelcast.simulator.worker.testcontainer;

import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.annotations.InjectTestContext;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class TestContainer_InjectTestContextTest extends TestContainer_AbstractTest {

    @Test
    public void testInjectTestContext() {
        TestContextTest test = new TestContextTest();
        testContainer = createTestContainer(test);

        assertNotNull(test.testContext);
        assertEquals(testContext, test.testContext);
    }

    @Test
    public void testInjectTestContext_withoutAnnotation() {
        TestContextTest test = new TestContextTest();
        testContainer = createTestContainer(test);

        assertNull(test.notAnnotatedTestContext);
    }

    private static class TestContextTest extends BaseTest {

        @InjectTestContext
        private TestContext testContext;

        @SuppressWarnings("unused")
        private TestContext notAnnotatedTestContext;
    }

    @Test(expected = IllegalTestException.class)
    public void testInjectTestContext_withIllegalFieldType() {
        IllegalFieldTypeTest test = new IllegalFieldTypeTest();
        testContainer = createTestContainer(test);
    }

    private static class IllegalFieldTypeTest extends BaseTest {

        @InjectTestContext
        private Object noProbeField;
    }
}
