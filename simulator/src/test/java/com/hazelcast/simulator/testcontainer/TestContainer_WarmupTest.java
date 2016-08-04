package com.hazelcast.simulator.testcontainer;

import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.testcontainer.AbstractTestContainerTest;
import com.hazelcast.simulator.testcontainer.TestPhase;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestContainer_WarmupTest extends AbstractTestContainerTest {

    @Test
    public void testLocalWarmup() throws Exception {
        WarmupTest test = new WarmupTest();
        testContainer = createTestContainer(test);
        testContainer.invoke(TestPhase.LOCAL_PREPARE);

        assertTrue(test.localPrepareCalled);
        assertFalse(test.globalPrepareCalled);
    }

    @Test
    public void testGlobalWarmup() throws Exception {
        WarmupTest test = new WarmupTest();
        testContainer = createTestContainer(test);
        testContainer.invoke(TestPhase.GLOBAL_PREPARE);

        assertFalse(test.localPrepareCalled);
        assertTrue(test.globalPrepareCalled);
    }

    private static class WarmupTest extends BaseTest {

        private boolean localPrepareCalled;
        private boolean globalPrepareCalled;

        @Prepare
        public void localPrepare() {
            localPrepareCalled = true;
        }

        @Prepare(global = true)
        public void globalPrepare() {
            globalPrepareCalled = true;
        }
    }
}
