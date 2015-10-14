package com.hazelcast.simulator.worker;

import com.hazelcast.simulator.probes.Probe;
import com.hazelcast.simulator.test.TestCase;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.TestPhase;
import com.hazelcast.simulator.test.annotations.Run;
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.SimulatorProbe;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.test.annotations.Warmup;
import com.hazelcast.simulator.worker.selector.OperationSelectorBuilder;
import com.hazelcast.simulator.worker.tasks.AbstractWorker;
import com.hazelcast.simulator.worker.tasks.IWorker;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static com.hazelcast.simulator.utils.CommonUtils.sleepMillis;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class TestContainerTest {

    private DummyTestContext testContext;
    private TestCase testCase;
    private TestContainer testContainer;

    @Before
    public void setUp() {
        testContext = new DummyTestContext();
        testCase = new TestCase("TestContainerTest");
    }

    @Test(expected = NullPointerException.class)
    public void testConstructor_testObject_isNull() {
        createTestContainer(null);
    }

    @Test(expected = NullPointerException.class)
    public void testConstructor_testContext_isNull() {
        new TestContainer(new DummyTest(), null, null);
    }

    @Test
    public void testConstructor_getTestContext() {
        testContainer = createTestContainer(new DummyTest());
        assertEquals(testContext, testContainer.getTestContext());
    }

    // =============================================================
    // =================== find annotations ========================
    // =============================================================

    @Test(expected = IllegalTestException.class)
    public void testRunAnnotationMissing() throws Exception {
        createTestContainer(new MissingRunAnnotationTest());
    }

    private static class MissingRunAnnotationTest {
    }

    @Test(expected = IllegalTestException.class)
    public void testTooManyMixedRunAnnotations() throws Exception {
        createTestContainer(new TooManyMixedRunAnnotationsTest());
    }

    private static class TooManyMixedRunAnnotationsTest {

        @Run
        public void run() {
        }

        @RunWithWorker
        public IWorker createWorker() {
            return null;
        }
    }

    @Test(expected = IllegalTestException.class)
    public void testDuplicateSetupAnnotation() {
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

    @Test
    public void testSetupAnnotationInheritance() throws Exception {
        // @Setup method will be called from child class, not from dummy class
        // @Run method will be called from dummy class, not from child class
        ChildWithOwnSetupMethodTest test = new ChildWithOwnSetupMethodTest();
        testContainer = createTestContainer(test);
        testContainer.invoke(TestPhase.SETUP);
        testContainer.invoke(TestPhase.RUN);

        // ChildWithOwnSetupMethodTest
        assertTrue(test.childSetupCalled);
        // DummySetupTest
        assertFalse(test.setupCalled);
        // DummyTest
        assertTrue(test.runCalled);
    }

    private static class ChildWithOwnSetupMethodTest extends DummySetupTest {
        boolean childSetupCalled;

        @Setup
        public void setUp(TestContext context) {
            this.context = context;
            this.childSetupCalled = true;
        }
    }

    @Test
    public void testRunAnnotationInheritance() throws Exception {
        // @Setup method will be called from dummy class, not from child class
        // @Run method will be called from child class, not from dummy class
        ChildWithOwnRunMethodTest test = new ChildWithOwnRunMethodTest();
        testContainer = createTestContainer(test);
        testContainer.invoke(TestPhase.SETUP);
        testContainer.invoke(TestPhase.RUN);

        // ChildWithOwnRunMethodTest
        assertTrue(test.childRunCalled);
        // DummySetupTest
        assertTrue(test.setupCalled);
        // DummyTest
        assertFalse(test.runCalled);
    }

    private static class ChildWithOwnRunMethodTest extends DummySetupTest {
        boolean childRunCalled;

        @Run
        void run() {
            this.childRunCalled = true;
        }
    }

    // ================================================
    // =================== run ========================
    // ================================================

    @Test
    public void testRun() throws Exception {
        DummyTest test = new DummyTest();
        testContainer = createTestContainer(test);
        testContainer.invoke(TestPhase.RUN);

        assertTrue(test.runCalled);
    }

    @Test
    public void testRunWithWorker() throws Exception {
        final RunWithWorkerTest test = new RunWithWorkerTest();
        testContainer = createTestContainer(test);
        Thread testStopper = new Thread() {
            @Override
            public void run() {
                while (!test.runWithWorkerCalled) {
                    sleepMillis(50);
                }
                testContext.stop();
            }
        };

        testStopper.start();
        testContainer.invoke(TestPhase.RUN);
        testStopper.join();

        assertTrue(test.runWithWorkerCalled);
    }

    private static class RunWithWorkerTest {

        private enum Operation {
            NOP
        }

        private static final OperationSelectorBuilder<Operation> BUILDER = new OperationSelectorBuilder<Operation>()
                .addDefaultOperation(Operation.NOP);

        volatile boolean runWithWorkerCalled;

        @RunWithWorker
        IWorker createWorker() {
            return new AbstractWorker<Operation>(BUILDER) {

                @Override
                protected void timeStep(Operation operation) throws Exception {
                    runWithWorkerCalled = true;
                }
            };
        }
    }

    @Test
    public void testRunWithIWorker() throws Exception {
        final RunWithIWorkerTest test = new RunWithIWorkerTest();
        testContainer = createTestContainer(test);
        Thread testStopper = new Thread() {
            @Override
            public void run() {
                while (!test.runWithWorkerCalled) {
                    sleepMillis(50);
                }
                testContext.stop();
            }
        };

        testStopper.start();
        testContainer.invoke(TestPhase.RUN);
        testStopper.join();

        assertTrue(test.runWithWorkerCalled);
    }

    private static class RunWithIWorkerTest {

        volatile boolean runWithWorkerCalled;

        @RunWithWorker
        IWorker createWorker() {
            return new IWorker() {

                @Override
                public void run() {
                    runWithWorkerCalled = true;
                }

                @Override
                public void afterCompletion() {
                }
            };
        }
    }

    // ==================================================
    // =================== setup ========================
    // ==================================================

    @Test()
    public void testSetupWithoutArguments() throws Exception {
        createTestContainer(new SetupWithoutArgumentsTest());
    }

    private static class SetupWithoutArgumentsTest extends DummyTest {

        @Setup
        public void setUp() {
        }
    }

    @Test()
    public void testSetupWithTestContextOnly() throws Exception {
        createTestContainer(new SetupWithTextContextOnlyTest());
    }

    private static class SetupWithTextContextOnlyTest extends DummyTest {

        @Setup
        public void setUp(TestContext testContext) {
        }
    }

    @Test(expected = IllegalTestException.class)
    public void testSetupWithProbe() throws Exception {
        createTestContainer(new SetupWithProbe());
    }

    private static class SetupWithProbe extends DummyTest {

        @Setup
        public void setUp(Probe probe) {
        }
    }

    @Test(expected = IllegalTestException.class)
    public void testIllegalSetupArguments() throws Exception {
        createTestContainer(new IllegalSetupArgumentsTest());
    }

    private static class IllegalSetupArgumentsTest extends DummyTest {

        @Setup
        public void setUp(TestContext testContext, Object wrongType) {
        }
    }

    @Test
    public void testSetupWithValidArguments() throws Exception {
        createTestContainer(new SetupWithValidArgumentsTest());
    }

    private static class SetupWithValidArgumentsTest extends DummyTest {

        @Setup
        public void setUp(TestContext testContext) {
        }
    }

    @Test
    public void testSetup() throws Exception {
        DummySetupTest test = new DummySetupTest();
        testContainer = createTestContainer(test);
        testContainer.invoke(TestPhase.SETUP);

        assertTrue(test.setupCalled);
        assertSame(testContext, test.context);
        assertFalse(test.runCalled);
    }

    // ===================================================
    // =================== probes ========================
    // ===================================================

    @Test
    public void testLocalProbeInjected() throws Exception {
        ProbeTest test = new ProbeTest();
        testContainer = createTestContainer(test);
        testContainer.invoke(TestPhase.SETUP);

        assertNotNull(test.simpleProbe);
    }

    @Test
    public void testProbeInjectIntervalProbeToField() throws Exception {
        ProbeTest test = new ProbeTest();
        testContainer = createTestContainer(test);

        assertNotNull(test.latencyProbe);
        assertTrue(testContainer.hasProbe("latencyProbe"));

        testContainer.invoke(TestPhase.RUN);
        Map<String, Probe> probeMap = testContainer.getProbeMap();
        assertTrue(probeMap.size() > 0);
        assertTrue(probeMap.keySet().contains("latencyProbe"));
    }

    @Test
    public void testProbeInjectExplicitlyNamedProbeToField() {
        ProbeTest test = new ProbeTest();
        testContainer = createTestContainer(test);

        assertNotNull(test.namedProbe);
        assertTrue(testContainer.hasProbe("explicitProbeInjectedToField"));
    }

    @SuppressWarnings("unused")
    private static class ProbeTest extends DummyTest {

        TestContext context;
        Probe simpleProbe;

        private Probe latencyProbe;

        @SimulatorProbe(name = "explicitProbeInjectedToField")
        private Probe namedProbe;

        @Setup
        public void setUp(TestContext context) {
            this.context = context;
        }

        @Run
        public void run() {
            latencyProbe.started();
            latencyProbe.done();
        }
    }

    // ===================================================
    // =================== warmup ========================
    // ===================================================

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

    private static class WarmupTest extends DummyTest {

        boolean localWarmupCalled;
        boolean globalWarmupCalled;

        @Warmup(global = false)
        void localTeardown() {
            localWarmupCalled = true;
        }

        @Warmup(global = true)
        void globalTeardown() {
            globalWarmupCalled = true;
        }
    }

    // ===================================================
    // =================== verify ========================
    // ===================================================

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

    private static class VerifyTest extends DummyTest {

        boolean localVerifyCalled;
        boolean globalVerifyCalled;

        @Verify(global = false)
        void localVerify() {
            localVerifyCalled = true;
        }

        @Verify(global = true)
        void globalVerify() {
            globalVerifyCalled = true;
        }
    }

    // =====================================================
    // =================== teardown ========================
    // =====================================================

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

    private static class TeardownTest extends DummyTest {

        boolean localTeardownCalled;
        boolean globalTeardownCalled;

        @Teardown(global = false)
        void localTeardown() {
            localTeardownCalled = true;
        }

        @Teardown(global = true)
        void globalTeardown() {
            globalTeardownCalled = true;
        }
    }

    // ==========================================================
    // =================== dummy classes ========================
    // ==========================================================

    private <T> TestContainer createTestContainer(T test) {
        return new TestContainer(test, testContext, testCase);
    }

    private static class DummyTest {

        boolean runCalled;

        @Run
        void run() {
            runCalled = true;
        }
    }

    private static class DummySetupTest extends DummyTest {

        TestContext context;
        boolean setupCalled;

        @Setup
        public void setUp(TestContext context) {
            this.context = context;
            this.setupCalled = true;
        }
    }
}
