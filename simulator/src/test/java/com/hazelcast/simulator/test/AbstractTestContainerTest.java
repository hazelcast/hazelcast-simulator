package com.hazelcast.simulator.test;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.simulator.test.annotations.Run;
import com.hazelcast.simulator.test.annotations.Setup;

import static org.mockito.Mockito.mock;

abstract class AbstractTestContainerTest {

    TestContext testContext = new TestContextImpl(mock(HazelcastInstance.class), "TestContainerTest");

    TestContainer testContainer;

    <T> TestContainer createTestContainer(T test) {
        return new TestContainer(testContext, test);
    }

    static class BaseTest {

        boolean runCalled;

        @Run
        void run() {
            runCalled = true;
        }
    }

    static class BaseSetupTest extends BaseTest {

        TestContext context;
        boolean setupCalled;

        @Setup
        public void setUp(TestContext context) {
            this.context = context;
            this.setupCalled = true;
        }
    }
}
