
package com.hazelcast.simulator.test;

import com.hazelcast.simulator.test.annotations.Run;
import com.hazelcast.simulator.test.annotations.Setup;
import org.junit.Test;

import static java.lang.String.format;
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
        new TestContainer(null, null);
    }

    @Test(expected = NullPointerException.class)
    public void testConstructor_withTestClassInstance_withNullTestContext() {
        new TestContainer(new BaseTest(), null, null);
    }

    @Test
    public void testGetTestContext() {
        testContainer = createTestContainer(new BaseTest());

        assertEquals(testContext, testContainer.getTestContext());
    }

    @Test
    public void testGetTestStartedTimestamp() throws Exception {
        long now = System.currentTimeMillis();
        testContainer = createTestContainer(new BaseTest());

        assertEquals(0, testContainer.getTestStartedTimestamp());

        testContainer.invoke(TestPhase.SETUP);
        testContainer.invoke(TestPhase.RUN);

        long testStartedTimestamp = testContainer.getTestStartedTimestamp();
        assertTrue(format("testStartedTimestamp should be >= %d, but was %d", now, testStartedTimestamp),
                testStartedTimestamp >= now);
    }

    @Test
    public void testIsRunning() throws Exception {
        testContainer = createTestContainer(new BaseTest());

        assertFalse(testContainer.isRunning());

        testContainer.invoke(TestPhase.SETUP);
        testContainer.invoke(TestPhase.RUN);

        assertFalse(testContainer.isRunning());
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
