
package com.hazelcast.simulator.test;

import com.hazelcast.simulator.test.annotations.Run;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.tests.SuccessTest;
import org.junit.Test;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TestContainer_BasicTest extends AbstractTestContainerTest {

    @Test
    public void testConstructor_withTestcase() {
        TestCase testCase = new TestCase("TestContainerNullContextTest");
        testCase.setProperty("class", SuccessTest.class.getName());

        testContainer = new TestContainer(testContext, testCase);

        assertNotNull(testContainer.getTestInstance());
        assertTrue(testContainer.getTestInstance() instanceof SuccessTest);
    }

    @Test
    public void testConstructor_withTestClassInstance() {
        SuccessTest test = new SuccessTest();
        testContainer = new TestContainer(testContext, test);

        assertEquals(test, testContainer.getTestInstance());
        assertTrue(testContainer.getTestInstance() instanceof SuccessTest);
    }

    @Test(expected = NullPointerException.class)
    public void testConstructor_withNullTestContext_withTestCase() {
        TestCase testCase = new TestCase("TestContainerNullContextTest");
        testCase.setProperty("class", SuccessTest.class.getName());

        new TestContainer(null, testCase);
    }

    @Test(expected = NullPointerException.class)
    public void testConstructor_withNullTestContext_withTestClassInstance() {
        new TestContainer(null, new BaseTest());
    }

    @Test(expected = NullPointerException.class)
    public void testConstructor_withNullTestContext_withTestClassInstance_withThreadCount() {
        new TestContainer(null, new BaseTest(), 3);
    }

    @Test(expected = NullPointerException.class)
    public void testConstructor_withNullTestClassInstance() {
        new TestContainer(testContext, null);
    }

    @Test(expected = NullPointerException.class)
    public void testConstructor_withNullTestClassInstance_withThreadCount() {
        new TestContainer(testContext, null, 3);
    }

    @Test
    public void testGetTestInstance() {
        BaseTest test = new BaseTest();
        testContainer = createTestContainer(test);

        assertEquals(test, testContainer.getTestInstance());
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
    public void testGetProbeMap() {
        testContainer = createTestContainer(new BaseTest());

        assertEquals(0, testContainer.getProbeMap().size());
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
