package com.hazelcast.simulator.test;

import com.hazelcast.simulator.test.annotations.InjectTestContext;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TestContainer_InjectTestContextTest extends AbstractTestContainerTest {

    @Test
    public void testInjectTestContext() {
        TestContextTest test = new TestContextTest();
        testContainer = createTestContainer(test);

        assertNotNull(test.testContext);
        assertEquals(testContext, test.testContext);
    }

    private static class TestContextTest extends BaseTest {

        @InjectTestContext
        private TestContext testContext;
    }
}
