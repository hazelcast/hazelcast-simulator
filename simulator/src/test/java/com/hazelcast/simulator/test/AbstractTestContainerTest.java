package com.hazelcast.simulator.test;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.simulator.test.annotations.Run;
import com.hazelcast.simulator.test.annotations.Setup;

import static org.mockito.Mockito.mock;

abstract class AbstractTestContainerTest {

    TestContext testContext = new TestContainerTestContext();
    TestCase testCase = new TestCase("TestContainerTest");

    TestContainer testContainer;

    private static class TestContainerTestContext implements TestContext {

        private final HazelcastInstance hazelcastInstance = mock(HazelcastInstance.class);

        private volatile boolean isStopped;

        @Override
        public HazelcastInstance getTargetInstance() {
            return hazelcastInstance;
        }

        @Override
        public String getTestId() {
            return "TestContainerTestContext";
        }

        @Override
        public boolean isStopped() {
            return isStopped;
        }

        @Override
        public void stop() {
            isStopped = true;
        }
    }

    <T> TestContainer createTestContainer(T test) {
        return new TestContainer(test, testContext, testCase);
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
