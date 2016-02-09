package com.hazelcast.simulator.test;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestContainer_BasicTest extends AbstractTestContainerTest {

    @Test(expected = NullPointerException.class)
    public void testConstructor_withNullTestObject() {
        createTestContainer(null);
    }

    @Test(expected = NullPointerException.class)
    public void testConstructor_withNullTestContext() {
        new TestContainer(new BaseTest(), null, null);
    }

    @Test
    public void testGetTestContext() {
        testContainer = createTestContainer(new BaseTest());

        assertEquals(testContext, testContainer.getTestContext());
    }
}
