package com.hazelcast.stabilizer.tests;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.stabilizer.tests.annotations.Run;
import com.hazelcast.stabilizer.tests.annotations.Setup;
import org.junit.Test;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class TestInvokerTest {

    @Test
    public void testRun() throws Throwable {
        DummyTest dummyTest = new DummyTest();
        TestInvoker invoker = new TestInvoker(dummyTest);
        invoker.run();

        assertTrue(dummyTest.runCalled);
    }

    @Test
    public void testSetup() throws Throwable {
        DummyTestContext testContext = new DummyTestContext();
        DummyTest dummyTest = new DummyTest();
        TestInvoker invoker = new TestInvoker(dummyTest);
        invoker.run();
        invoker.setup(testContext);

        assertTrue(dummyTest.setupCalled);
        assertSame(testContext, dummyTest.context);
    }

    static class DummyTest {
        boolean runCalled;
        boolean setupCalled;
        TestContext context;

        @Setup
        void setup(TestContext context) {
            this.context = context;
            this.setupCalled = true;
        }

        @Run
        void run() {
            runCalled = true;
        }
    }

    static class DummyTestContext implements TestContext {
        @Override
        public HazelcastInstance getTargetInstance() {
            return null;
        }

        @Override
        public String getTestId() {
            return null;
        }

        @Override
        public boolean isStopped() {
            return false;
        }
    }
}
