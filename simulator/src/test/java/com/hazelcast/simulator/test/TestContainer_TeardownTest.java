package com.hazelcast.simulator.test;

import com.hazelcast.simulator.test.annotations.Teardown;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestContainer_TeardownTest extends AbstractTestContainerTest {

    @Test
    public void testLocalTeardown() throws Exception {
        TeardownTest test = new TeardownTest();
        testContainer = createTestContainer(test);
        testContainer.invoke(TestPhase.LOCAL_TEARDOWN);

        assertTrue(test.localTeardownCalled);
        assertFalse(test.globalTeardownCalled);
    }

    @Test
    public void testGlobalTeardown() throws Exception {
        TeardownTest test = new TeardownTest();
        testContainer = createTestContainer(test);
        testContainer.invoke(TestPhase.GLOBAL_TEARDOWN);

        assertFalse(test.localTeardownCalled);
        assertTrue(test.globalTeardownCalled);
    }

    private static class TeardownTest extends BaseTest {

        private boolean localTeardownCalled;
        private boolean globalTeardownCalled;

        @Teardown
        void localTeardown() {
            localTeardownCalled = true;
        }

        @Teardown(global = true)
        void globalTeardown() {
            globalTeardownCalled = true;
        }
    }
}
