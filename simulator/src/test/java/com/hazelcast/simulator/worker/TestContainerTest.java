package com.hazelcast.simulator.worker;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.simulator.common.messaging.Message;
import com.hazelcast.simulator.probes.probes.IntervalProbe;
import com.hazelcast.simulator.probes.probes.ProbesConfiguration;
import com.hazelcast.simulator.probes.probes.ProbesType;
import com.hazelcast.simulator.probes.probes.SimpleProbe;
import com.hazelcast.simulator.probes.probes.impl.DisabledProbe;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.annotations.Name;
import com.hazelcast.simulator.test.annotations.Performance;
import com.hazelcast.simulator.test.annotations.Receive;
import com.hazelcast.simulator.test.annotations.Run;
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.test.annotations.Warmup;
import com.hazelcast.simulator.worker.selector.OperationSelectorBuilder;
import com.hazelcast.simulator.worker.tasks.AbstractWorker;
import com.hazelcast.simulator.worker.tasks.IWorker;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Map;

import static com.hazelcast.simulator.utils.CommonUtils.sleepMillis;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class TestContainerTest {

    private DummyTestContext testContext;
    private ProbesConfiguration probesConfiguration;
    private TestContainer<DummyTestContext> invoker;

    @Before
    public void init() {
        testContext = new DummyTestContext();
        probesConfiguration = new ProbesConfiguration();
    }

    // =============================================================
    // =================== find annotations ========================
    // =============================================================

    @Test(expected = IllegalTestException.class)
    public void testRunAnnotationMissing() throws Exception {
        new TestContainer<DummyTestContext>(new MissingRunAnnotationTest(), testContext, probesConfiguration);
    }

    private static class MissingRunAnnotationTest {
    }

    @Test(expected = IllegalTestException.class)
    public void testTooManyMixedRunAnnotations() throws Exception {
        new TestContainer<DummyTestContext>(new TooManyMixedRunAnnotationsTest(), testContext, probesConfiguration);
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
        new TestContainer<DummyTestContext>(new DuplicateSetupAnnotationTest(), testContext, probesConfiguration);
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
        invoker = new TestContainer<DummyTestContext>(test, testContext, probesConfiguration);
        invoker.setUp();
        invoker.run();

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
        invoker = new TestContainer<DummyTestContext>(test, testContext, probesConfiguration);
        invoker.setUp();
        invoker.run();

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
        invoker = new TestContainer<DummyTestContext>(test, testContext, probesConfiguration);
        invoker.run();

        assertTrue(test.runCalled);
    }

    @Test
    public void testRunWithWorker() throws Exception {
        final RunWithWorkerTest test = new RunWithWorkerTest();
        invoker = new TestContainer<DummyTestContext>(test, testContext, probesConfiguration);
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
        invoker.run();
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
                protected void timeStep(Operation operation) {
                    runWithWorkerCalled = true;
                }
            };
        }
    }

    @Test
    public void testRunWithIWorker() throws Exception {
        final RunWithIWorkerTest test = new RunWithIWorkerTest();
        invoker = new TestContainer<DummyTestContext>(test, testContext, probesConfiguration);
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
        invoker.run();
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
        new TestContainer<DummyTestContext>(new SetupWithoutArgumentsTest(), testContext, probesConfiguration);
    }

    private static class SetupWithoutArgumentsTest extends DummyTest {

        @Setup
        public void setUp() {
        }
    }

    @Test()
    public void testSetupWithTestContextOnly() throws Exception {
        new TestContainer<DummyTestContext>(new SetupWithTextContextOnlyTest(), testContext, probesConfiguration);
    }

    private static class SetupWithTextContextOnlyTest extends DummyTest {

        @Setup
        public void setUp(TestContext testContext) {
        }
    }

    @Test(expected = IllegalTestException.class)
    public void testSetupWithSimpleProbeOnly() throws Exception {
        new TestContainer<DummyTestContext>(new SetupWithSimpleProbeOnly(), testContext, probesConfiguration);
    }

    private static class SetupWithSimpleProbeOnly extends DummyTest {

        @Setup
        public void setUp(SimpleProbe simpleProbe) {
        }
    }

    @Test(expected = IllegalTestException.class)
    public void testIllegalSetupArguments() throws Exception {
        new TestContainer<DummyTestContext>(new IllegalSetupArgumentsTest(), testContext, probesConfiguration);
    }

    private static class IllegalSetupArgumentsTest extends DummyTest {

        @Setup
        public void setUp(TestContext testContext, Object wrongType) {
        }
    }

    @Test
    public void testSetupWithValidArguments() throws Exception {
        new TestContainer<DummyTestContext>(new SetupWithValidArgumentsTest(), testContext, probesConfiguration);
    }

    private static class SetupWithValidArgumentsTest extends DummyTest {

        @Setup
        public void setUp(TestContext testContext, SimpleProbe simpleProbe, IntervalProbe intervalProbe) {
        }
    }

    @Test
    public void testSetup() throws Exception {
        DummySetupTest test = new DummySetupTest();
        invoker = new TestContainer<DummyTestContext>(test, testContext, probesConfiguration);
        invoker.setUp();

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
        invoker = new TestContainer<DummyTestContext>(test, testContext, probesConfiguration);
        invoker.setUp();

        assertNotNull(test.simpleProbe);
    }

    @Test
    public void testProbeExplicitNameSetViaAnnotation() throws Exception {
        ProbeTest test = new ProbeTest();
        probesConfiguration.addConfig("explicitProbeName", ProbesType.THROUGHPUT.string);
        invoker = new TestContainer<DummyTestContext>(test, testContext, probesConfiguration);
        Map probeResults = invoker.getProbeResults();

        assertTrue(probeResults.keySet().contains("explicitProbeName"));
    }

    @Test
    public void testProbeImplicitName() throws Exception {
        ProbeTest test = new ProbeTest();
        probesConfiguration.addConfig("Probe2", ProbesType.THROUGHPUT.string);
        invoker = new TestContainer<DummyTestContext>(test, testContext, probesConfiguration);
        Map probeResults = invoker.getProbeResults();

        assertTrue(probeResults.keySet().contains("Probe2"));
    }

    @Test
    public void testProbeInjectSimpleProbeToField() {
        ProbeTest test = new ProbeTest();
        probesConfiguration.addConfig("throughputProbe", ProbesType.THROUGHPUT.string);
        invoker = new TestContainer<DummyTestContext>(test, testContext, probesConfiguration);
        Map probeResults = invoker.getProbeResults();

        assertNotNull(test.throughputProbe);
        assertTrue(probeResults.keySet().contains("throughputProbe"));
    }

    @Test
    public void testProbeInjectIntervalProbeToField() {
        ProbeTest test = new ProbeTest();
        probesConfiguration.addConfig("latencyProbe", ProbesType.LATENCY.string);
        invoker = new TestContainer<DummyTestContext>(test, testContext, probesConfiguration);
        Map probeResults = invoker.getProbeResults();

        assertNotNull(test.latencyProbe);
        assertTrue(probeResults.keySet().contains("latencyProbe"));
    }

    @Test
    public void testProbeInjectExplicitlyNamedProbeToField() {
        ProbeTest test = new ProbeTest();
        probesConfiguration.addConfig("explicitProbeInjectedToField", ProbesType.THROUGHPUT.string);
        invoker = new TestContainer<DummyTestContext>(test, testContext, probesConfiguration);
        Map probeResults = invoker.getProbeResults();

        assertNotNull(test.fooProbe);
        assertTrue(probeResults.keySet().contains("explicitProbeInjectedToField"));
    }

    @Test
    public void testProbeInjectDisabledToField() {
        ProbeTest test = new ProbeTest();
        new TestContainer<DummyTestContext>(test, testContext, probesConfiguration);

        assertNotNull(test.disabled);
        assertTrue(test.disabled instanceof DisabledProbe);
    }

    @SuppressWarnings("unused")
    private static class ProbeTest extends DummyTest {
        TestContext context;
        SimpleProbe simpleProbe;

        private SimpleProbe throughputProbe;
        private IntervalProbe latencyProbe;

        @Name("explicitProbeInjectedToField")
        private SimpleProbe fooProbe;
        private IntervalProbe disabled;

        @Setup
        public void setUp(TestContext context, @Name("explicitProbeName") SimpleProbe probe1, SimpleProbe probe2) {
            this.context = context;
            this.simpleProbe = probe1;
        }
    }

    // ===================================================
    // =================== warmup ========================
    // ===================================================

    @Test
    public void testLocalWarmup() throws Exception {
        WarmupTest test = new WarmupTest();
        invoker = new TestContainer<DummyTestContext>(test, testContext, probesConfiguration);
        invoker.localWarmup();

        assertTrue(test.localWarmupCalled);
        assertFalse(test.globalWarmupCalled);
    }

    @Test
    public void testGlobalWarmup() throws Exception {
        WarmupTest test = new WarmupTest();
        invoker = new TestContainer<DummyTestContext>(test, testContext, probesConfiguration);
        invoker.globalWarmup();

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
        invoker = new TestContainer<DummyTestContext>(test, testContext, probesConfiguration);
        invoker.localVerify();

        assertTrue(test.localVerifyCalled);
        assertFalse(test.globalVerifyCalled);
    }

    @Test
    public void testGlobalVerify() throws Exception {
        VerifyTest test = new VerifyTest();
        invoker = new TestContainer<DummyTestContext>(test, testContext, probesConfiguration);
        invoker.globalVerify();

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
        invoker = new TestContainer<DummyTestContext>(test, testContext, probesConfiguration);
        invoker.localTeardown();

        assertTrue(test.localTeardownCalled);
        assertFalse(test.globalTeardownCalled);
    }

    @Test
    public void testGlobalTeardown() throws Exception {
        TeardownTest test = new TeardownTest();
        invoker = new TestContainer<DummyTestContext>(test, testContext, probesConfiguration);
        invoker.globalTeardown();

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

    // ========================================================
    // =================== performance ========================
    // ========================================================

    @Test
    public void testPerformance() throws Exception {
        PerformanceTest test = new PerformanceTest();
        invoker = new TestContainer<DummyTestContext>(test, testContext, probesConfiguration);
        long count = invoker.getOperationCount();

        assertEquals(20, count);
    }

    private static class PerformanceTest {

        @Performance
        public long getCount() {
            return 20;
        }

        @Run
        void run() {
        }
    }

    // ====================================================
    // =================== receive ========================
    // ====================================================

    @Test
    public void testMessageReceiver() throws Exception {
        ReceiveTest test = new ReceiveTest();
        invoker = new TestContainer<DummyTestContext>(test, testContext, probesConfiguration);
        Message message = Mockito.mock(Message.class);
        invoker.sendMessage(message);

        assertEquals(message, test.messagePassed);
    }

    private static class ReceiveTest extends DummyTest {

        Message messagePassed;

        @Receive
        public void receive(Message message) {
            messagePassed = message;
        }
    }

    // ==========================================================
    // =================== dummy classes ========================
    // ==========================================================

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

    private static class DummyTestContext implements TestContext {

        volatile boolean isStopped;

        @Override
        public HazelcastInstance getTargetInstance() {
            return null;
        }

        @Override
        public String getTestId() {
            return "";
        }

        @Override
        public boolean isStopped() {
            return isStopped;
        }

        @Override
        public void stop() {
            isStopped = true;
        }
    }
}
