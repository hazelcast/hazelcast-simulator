package com.hazelcast.stabilizer.worker;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.stabilizer.common.messaging.Message;
import com.hazelcast.stabilizer.probes.probes.IntervalProbe;
import com.hazelcast.stabilizer.probes.probes.ProbesConfiguration;
import com.hazelcast.stabilizer.probes.probes.SimpleProbe;
import com.hazelcast.stabilizer.probes.probes.impl.DisabledProbe;
import com.hazelcast.stabilizer.test.annotations.RunWithWorker;
import com.hazelcast.stabilizer.test.annotations.Teardown;
import com.hazelcast.stabilizer.test.annotations.Warmup;
import com.hazelcast.stabilizer.test.TestContext;
import com.hazelcast.stabilizer.test.annotations.Name;
import com.hazelcast.stabilizer.test.annotations.Performance;
import com.hazelcast.stabilizer.test.annotations.Receive;
import com.hazelcast.stabilizer.test.annotations.Run;
import com.hazelcast.stabilizer.test.annotations.Setup;
import com.hazelcast.stabilizer.test.annotations.Verify;
import com.hazelcast.stabilizer.worker.selector.OperationSelectorBuilder;
import com.hazelcast.stabilizer.worker.tasks.AbstractWorkerTask;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Map;

import static com.hazelcast.stabilizer.utils.CommonUtils.sleepMillis;
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
    public void testRunAnnotationMissing() throws Throwable {
        new TestContainer<DummyTestContext>(new MissingRunAnnotationTest(), testContext, probesConfiguration);
    }

    private static class MissingRunAnnotationTest {
    }

    @Test(expected = IllegalTestException.class)
    public void testTooManyMixedRunAnnotations() throws Throwable {
        new TestContainer<DummyTestContext>(new TooManyMixedRunAnnotationsTest(), testContext, probesConfiguration);
    }

    private static class TooManyMixedRunAnnotationsTest {
        @Run
        public void run() {
        }

        @RunWithWorker
        public AbstractWorkerTask createBaseWorker() {
            return null;
        }
    }

    @Test(expected = IllegalTestException.class)
    public void testDuplicateSetupAnnotation() {
        new TestContainer<DummyTestContext>(new DuplicateSetupAnnotationTest(), testContext, probesConfiguration);
    }

    @SuppressWarnings("unused")
    private static class DuplicateSetupAnnotationTest {
        @Setup
        void setup() {
        }

        @Setup
        void anotherSetup() {
        }
    }

    @Test
    public void testSetupAnnotationInheritance() throws Throwable {
        // @Setup method will be called from child class, not from dummy class
        // @Run method will be called from dummy class, not from child class
        ChildWithOwnSetupMethodTest test = new ChildWithOwnSetupMethodTest();
        invoker = new TestContainer<DummyTestContext>(test, testContext, probesConfiguration);
        invoker.setup();
        invoker.run();

        assertTrue(test.childSetupCalled); // ChildWithOwnSetupMethodTest
        assertFalse(test.setupCalled); // DummySetupTest
        assertTrue(test.runCalled); // DummyTest
    }

    private static class ChildWithOwnSetupMethodTest extends DummySetupTest {
        boolean childSetupCalled;

        @Setup
        void setup(TestContext context) {
            this.context = context;
            this.childSetupCalled = true;
        }
    }

    @Test
    public void testRunAnnotationInheritance() throws Throwable {
        // @Setup method will be called from dummy class, not from child class
        // @Run method will be called from child class, not from dummy class
        ChildWithOwnRunMethodTest test = new ChildWithOwnRunMethodTest();
        invoker = new TestContainer<DummyTestContext>(test, testContext, probesConfiguration);
        invoker.setup();
        invoker.run();

        assertTrue(test.childRunCalled); // ChildWithOwnRunMethodTest
        assertTrue(test.setupCalled); // DummySetupTest
        assertFalse(test.runCalled); // DummyTest
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
    public void testRun() throws Throwable {
        DummyTest test = new DummyTest();
        invoker = new TestContainer<DummyTestContext>(test, testContext, probesConfiguration);
        invoker.run();

        assertTrue(test.runCalled);
    }

    @Test
    public void testRunWithBaseWorker() throws Throwable {
        RunWithBaseWorkerTest test = new RunWithBaseWorkerTest();
        invoker = new TestContainer<DummyTestContext>(test, testContext, probesConfiguration);
        new Thread() {
            @Override
            public void run() {
                super.run();
                sleepMillis(50);
                testContext.stop();
            }
        }.start();
        invoker.run();

        assertTrue(test.runWithWorkerCalled);
    }

    private static class RunWithBaseWorkerTest {
        private enum Operation {
            NOP
        }

        private static final OperationSelectorBuilder<Operation> builder = new OperationSelectorBuilder<Operation>()
                .addDefaultOperation(Operation.NOP);

        boolean runWithWorkerCalled;

        @RunWithWorker
        AbstractWorkerTask<Operation> createBaseWorker() {
            return new AbstractWorkerTask<Operation>(builder) {
                @Override
                protected void doRun(Operation operation) {
                    runWithWorkerCalled = true;
                }
            };
        }
    }

    // ==================================================
    // =================== setup ========================
    // ==================================================

    @Test()
    public void testSetupWithoutArguments() throws Throwable {
        new TestContainer<DummyTestContext>(new SetupWithoutArgumentsTest(), testContext, probesConfiguration);
    }

    @SuppressWarnings("unused")
    private static class SetupWithoutArgumentsTest extends DummyTest {
        @Setup
        void setup() {
        }
    }

    @Test()
    public void testSetupWithTestContextOnly() throws Throwable {
        new TestContainer<DummyTestContext>(new SetupWithTextContextOnlyTest(), testContext, probesConfiguration);
    }

    @SuppressWarnings("unused")
    private static class SetupWithTextContextOnlyTest extends DummyTest {
        @Setup
        void setup(TestContext testContext) {
        }
    }

    @Test(expected = IllegalTestException.class)
    public void testSetupWithSimpleProbeOnly() throws Throwable {
        new TestContainer<DummyTestContext>(new SetupWithSimpleProbeOnly(), testContext, probesConfiguration);
    }

    @SuppressWarnings("unused")
    private static class SetupWithSimpleProbeOnly extends DummyTest {
        @Setup
        void setup(SimpleProbe simpleProbe) {
        }
    }

    @Test(expected = IllegalTestException.class)
    public void testIllegalSetupArguments() throws Throwable {
        new TestContainer<DummyTestContext>(new IllegalSetupArgumentsTest(), testContext, probesConfiguration);
    }

    @SuppressWarnings("unused")
    private static class IllegalSetupArgumentsTest extends DummyTest {
        @Setup
        void setup(TestContext testContext, Object wrongType) {
        }
    }

    @Test
    public void testSetupWithValidArguments() throws Throwable {
        new TestContainer<DummyTestContext>(new SetupWithValidArgumentsTest(), testContext, probesConfiguration);
    }

    @SuppressWarnings("unused")
    private static class SetupWithValidArgumentsTest extends DummyTest {
        @Setup
        void setup(TestContext testContext, SimpleProbe simpleProbe, IntervalProbe intervalProbe) {
        }
    }

    @Test
    public void testSetup() throws Throwable {
        DummySetupTest test = new DummySetupTest();
        invoker = new TestContainer<DummyTestContext>(test, testContext, probesConfiguration);
        invoker.setup();

        assertTrue(test.setupCalled);
        assertSame(testContext, test.context);
        assertFalse(test.runCalled);
    }

    // ===================================================
    // =================== probes ========================
    // ===================================================

    @Test
    public void testLocalProbeInjected() throws Throwable {
        ProbeTest test = new ProbeTest();
        invoker = new TestContainer<DummyTestContext>(test, testContext, probesConfiguration);
        invoker.setup();

        assertNotNull(test.simpleProbe);
    }

    @Test
    public void testProbeExplicitNameSetViaAnnotation() throws Throwable {
        ProbeTest test = new ProbeTest();
        probesConfiguration.addConfig("explicitProbeName", "throughput");
        invoker = new TestContainer<DummyTestContext>(test, testContext, probesConfiguration);
        Map probeResults = invoker.getProbeResults();

        assertTrue(probeResults.keySet().contains("explicitProbeName"));
    }

    @Test
    public void testProbeImplicitName() throws Throwable {
        ProbeTest test = new ProbeTest();
        probesConfiguration.addConfig("Probe2", "throughput");
        invoker = new TestContainer<DummyTestContext>(test, testContext, probesConfiguration);
        Map probeResults = invoker.getProbeResults();

        assertTrue(probeResults.keySet().contains("Probe2"));
    }

    @Test
    public void testProbeInjectSimpleProbeToField() {
        ProbeTest test = new ProbeTest();
        probesConfiguration.addConfig("throughputProbe", "throughput");
        invoker = new TestContainer<DummyTestContext>(test, testContext, probesConfiguration);
        Map probeResults = invoker.getProbeResults();

        assertNotNull(test.throughputProbe);
        assertTrue(probeResults.keySet().contains("throughputProbe"));
    }

    @Test
    public void testProbeInjectIntervalProbeToField() {
        ProbeTest test = new ProbeTest();
        probesConfiguration.addConfig("latencyProbe", "latency");
        invoker = new TestContainer<DummyTestContext>(test, testContext, probesConfiguration);
        Map probeResults = invoker.getProbeResults();

        assertNotNull(test.latencyProbe);
        assertTrue(probeResults.keySet().contains("latencyProbe"));
    }

    @Test
    public void testProbeInjectExplicitlyNamedProbeToField() {
        ProbeTest test = new ProbeTest();
        probesConfiguration.addConfig("explicitProbeInjectedToField", "throughput");
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
        void setup(TestContext context, @Name("explicitProbeName") SimpleProbe probe1, SimpleProbe probe2) {
            this.context = context;
            this.simpleProbe = probe1;
        }
    }

    // ===================================================
    // =================== warmup ========================
    // ===================================================

    @Test
    public void testLocalWarmup() throws Throwable {
        WarmupTest test = new WarmupTest();
        invoker = new TestContainer<DummyTestContext>(test, testContext, probesConfiguration);
        invoker.localWarmup();

        assertTrue(test.localWarmupCalled);
        assertFalse(test.globalWarmupCalled);
    }

    @Test
    public void testGlobalWarmup() throws Throwable {
        WarmupTest test = new WarmupTest();
        invoker = new TestContainer<DummyTestContext>(test, testContext, probesConfiguration);
        invoker.globalWarmup();

        assertFalse(test.localWarmupCalled);
        assertTrue(test.globalWarmupCalled);
    }

    @SuppressWarnings("unused")
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
    public void testLocalVerify() throws Throwable {
        VerifyTest test = new VerifyTest();
        invoker = new TestContainer<DummyTestContext>(test, testContext, probesConfiguration);
        invoker.localVerify();

        assertTrue(test.localVerifyCalled);
        assertFalse(test.globalVerifyCalled);
    }

    @Test
    public void testGlobalVerify() throws Throwable {
        VerifyTest test = new VerifyTest();
        invoker = new TestContainer<DummyTestContext>(test, testContext, probesConfiguration);
        invoker.globalVerify();

        assertFalse(test.localVerifyCalled);
        assertTrue(test.globalVerifyCalled);
    }

    @SuppressWarnings("unused")
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
    public void testLocalTeardown() throws Throwable {
        TeardownTest test = new TeardownTest();
        invoker = new TestContainer<DummyTestContext>(test, testContext, probesConfiguration);
        invoker.localTeardown();

        assertTrue(test.localTeardownCalled);
        assertFalse(test.globalTeardownCalled);
    }

    @Test
    public void testGlobalTeardown() throws Throwable {
        TeardownTest test = new TeardownTest();
        invoker = new TestContainer<DummyTestContext>(test, testContext, probesConfiguration);
        invoker.globalTeardown();

        assertFalse(test.localTeardownCalled);
        assertTrue(test.globalTeardownCalled);
    }

    @SuppressWarnings("unused")
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
    public void testPerformance() throws Throwable {
        PerformanceTest test = new PerformanceTest();
        invoker = new TestContainer<DummyTestContext>(test, testContext, probesConfiguration);
        long count = invoker.getOperationCount();

        assertEquals(20, count);
    }

    @SuppressWarnings("unused")
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
    public void testMessageReceiver() throws Throwable {
        ReceiveTest test = new ReceiveTest();
        invoker = new TestContainer<DummyTestContext>(test, testContext, probesConfiguration);
        Message message = Mockito.mock(Message.class);
        invoker.sendMessage(message);

        assertEquals(message, test.messagePassed);
    }

    @SuppressWarnings("unused")
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
        void setup(TestContext context) {
            this.context = context;
            this.setupCalled = true;
        }
    }

    private static class DummyTestContext implements TestContext {
        volatile boolean isStopped = false;

        @Override
        public HazelcastInstance getTargetInstance() {
            return null;
        }

        @Override
        public String getTestId() {
            return null;
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
