package com.hazelcast.simulator.worker.testcontainer;

import com.hazelcast.simulator.common.TestPhase;
import com.hazelcast.simulator.test.annotations.Prepare;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestContainer_PrepareTest extends TestContainer_AbstractTest {

    @Test
    public void testLocalWarmup() throws Exception {
        PrepareTest test = new PrepareTest();
        testContainer = createTestContainer(test);
        testContainer.invoke(TestPhase.LOCAL_PREPARE);

        assertTrue(test.localPrepareCalled);
        assertFalse(test.globalPrepareCalled);
    }

    @Test
    public void testGlobalWarmup() throws Exception {
        PrepareTest test = new PrepareTest();
        testContainer = createTestContainer(test);
        testContainer.invoke(TestPhase.GLOBAL_PREPARE);

        assertFalse(test.localPrepareCalled);
        assertTrue(test.globalPrepareCalled);
    }

    private static class PrepareTest extends BaseTest {

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
