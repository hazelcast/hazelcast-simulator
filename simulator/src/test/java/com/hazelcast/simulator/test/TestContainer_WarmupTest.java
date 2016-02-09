package com.hazelcast.simulator.test;

import com.hazelcast.simulator.test.annotations.Warmup;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestContainer_WarmupTest extends AbstractTestContainerTest {

    @Test
    public void testLocalWarmup() throws Exception {
        WarmupTest test = new WarmupTest();
        testContainer = createTestContainer(test);
        testContainer.invoke(TestPhase.LOCAL_WARMUP);

        assertTrue(test.localWarmupCalled);
        assertFalse(test.globalWarmupCalled);
    }

    @Test
    public void testGlobalWarmup() throws Exception {
        WarmupTest test = new WarmupTest();
        testContainer = createTestContainer(test);
        testContainer.invoke(TestPhase.GLOBAL_WARMUP);

        assertFalse(test.localWarmupCalled);
        assertTrue(test.globalWarmupCalled);
    }

    private static class WarmupTest extends BaseTest {

        private boolean localWarmupCalled;
        private boolean globalWarmupCalled;

        @Warmup
        void localTeardown() {
            localWarmupCalled = true;
        }

        @Warmup(global = true)
        void globalTeardown() {
            globalWarmupCalled = true;
        }
    }
}
