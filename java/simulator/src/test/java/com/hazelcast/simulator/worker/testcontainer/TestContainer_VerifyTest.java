package com.hazelcast.simulator.worker.testcontainer;

import com.hazelcast.simulator.common.TestPhase;
import com.hazelcast.simulator.test.annotations.Verify;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestContainer_VerifyTest extends TestContainer_AbstractTest {

    @Test
    public void testLocalVerify() throws Exception {
        VerifyTest test = new VerifyTest();
        testContainer = createTestContainer(test);
        testContainer.invoke(TestPhase.LOCAL_VERIFY);

        assertTrue(test.localVerifyCalled);
        assertFalse(test.globalVerifyCalled);
    }

    @Test
    public void testGlobalVerify() throws Exception {
        VerifyTest test = new VerifyTest();
        testContainer = createTestContainer(test);
        testContainer.invoke(TestPhase.GLOBAL_VERIFY);

        assertFalse(test.localVerifyCalled);
        assertTrue(test.globalVerifyCalled);
    }

    private static class VerifyTest extends BaseTest {

        private boolean localVerifyCalled;
        private boolean globalVerifyCalled;

        @Verify(global = false)
        public void localVerify() {
            localVerifyCalled = true;
        }

        @Verify
        public void globalVerify() {
            globalVerifyCalled = true;
        }
    }
}
