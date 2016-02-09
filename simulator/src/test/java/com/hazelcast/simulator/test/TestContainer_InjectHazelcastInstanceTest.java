package com.hazelcast.simulator.test;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.simulator.test.annotations.InjectHazelcastInstance;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TestContainer_InjectHazelcastInstanceTest extends AbstractTestContainerTest {

    @Test
    public void testInjectTestContext() {
        HazelcastInstanceTest test = new HazelcastInstanceTest();
        testContainer = createTestContainer(test);

        assertNotNull(test.hazelcastInstance);
        assertEquals(testContext.getTargetInstance(), test.hazelcastInstance);
    }

    private static class HazelcastInstanceTest extends BaseTest {

        @InjectHazelcastInstance
        private HazelcastInstance hazelcastInstance;
    }
}
