package com.hazelcast.simulator.test;

import com.hazelcast.simulator.test.annotations.Run;
import com.hazelcast.simulator.test.annotations.Setup;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

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

    @Test
    public void testAnnotationInheritance_withSetupInBaseClass_withRunInChildClass() throws Exception {
        // @Setup method will be called from base class, not from child class
        // @Run method will be called from child class, not from base class
        ChildWithOwnRunMethodTest test = new ChildWithOwnRunMethodTest();
        testContainer = createTestContainer(test);
        testContainer.invoke(TestPhase.SETUP);
        testContainer.invoke(TestPhase.RUN);

        // ChildWithOwnRunMethodTest
        assertTrue(test.childRunCalled);
        // BaseSetupTest
        assertTrue(test.setupCalled);
        // BaseTest
        assertFalse(test.runCalled);
    }

    private static class ChildWithOwnRunMethodTest extends BaseSetupTest {

        private boolean childRunCalled;

        @Run
        void run() {
            this.childRunCalled = true;
        }
    }

    @Test
    public void testAnnotationInheritance_withSetupInChildClass_withRunInBaseClass() throws Exception {
        // @Setup method will be called from child class, not from base class
        // @Run method will be called from base class, not from child class
        ChildWithOwnSetupMethodTest test = new ChildWithOwnSetupMethodTest();
        testContainer = createTestContainer(test);
        testContainer.invoke(TestPhase.SETUP);
        testContainer.invoke(TestPhase.RUN);

        // ChildWithOwnSetupMethodTest
        assertTrue(test.childSetupCalled);
        // BaseSetupTest
        assertFalse(test.setupCalled);
        // BaseTest
        assertTrue(test.runCalled);
    }

    private static class ChildWithOwnSetupMethodTest extends BaseSetupTest {

        private boolean childSetupCalled;

        @Setup
        public void setUp(TestContext context) {
            this.context = context;
            this.childSetupCalled = true;
        }
    }
}
