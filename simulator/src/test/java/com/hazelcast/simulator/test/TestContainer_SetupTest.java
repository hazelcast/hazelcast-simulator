package com.hazelcast.simulator.test;

import com.hazelcast.simulator.probes.Probe;
import com.hazelcast.simulator.test.annotations.Setup;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class TestContainer_SetupTest extends AbstractTestContainerTest {

    @Test
    public void testSetup() throws Exception {
        BaseSetupTest test = new BaseSetupTest();
        testContainer = createTestContainer(test);
        testContainer.invoke(TestPhase.SETUP);

        assertTrue(test.setupCalled);
        assertSame(testContext, test.context);
        assertFalse(test.runCalled);
    }

    @Test
    public void testSetup_withAnnotationInheritance() throws Exception {
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

    @Test
    public void testSetup_withoutArguments() {
        createTestContainer(new SetupWithoutArgumentsTest());
    }

    private static class SetupWithoutArgumentsTest extends BaseTest {

        @Setup
        public void setUp() {
        }
    }

    @Test
    public void testSetup_withValidArguments() {
        createTestContainer(new SetupWithValidArgumentsTest());
    }

    private static class SetupWithValidArgumentsTest extends BaseTest {

        @Setup
        public void setUp(TestContext testContext) {
        }
    }

    @Test(expected = IllegalTestException.class)
    public void testSetup_withProbe() {
        createTestContainer(new SetupWithProbe());
    }

    private static class SetupWithProbe extends BaseTest {

        @Setup
        public void setUp(Probe probe) {
        }
    }

    @Test(expected = IllegalTestException.class)
    public void testSetup_withIllegalSetupArguments() {
        createTestContainer(new IllegalSetupArgumentsTest());
    }

    private static class IllegalSetupArgumentsTest extends BaseTest {

        @Setup
        public void setUp(TestContext testContext, Object wrongType) {
        }
    }

    @Test(expected = IllegalTestException.class)
    public void testSetup_withDuplicateAnnotation() {
        createTestContainer(new DuplicateSetupAnnotationTest());
    }

    private static class DuplicateSetupAnnotationTest {

        @Setup
        public void setUp() {
        }

        @Setup
        public void anotherSetup() {
        }
    }
}
