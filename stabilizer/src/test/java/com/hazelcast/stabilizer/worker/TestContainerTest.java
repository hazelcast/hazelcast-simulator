package com.hazelcast.stabilizer.worker;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.stabilizer.common.messaging.Message;
import com.hazelcast.stabilizer.common.probes.ProbesConfiguration;
import com.hazelcast.stabilizer.common.probes.SimpleProbe;
import com.hazelcast.stabilizer.tests.IllegalTestException;
import com.hazelcast.stabilizer.tests.TestContext;
import com.hazelcast.stabilizer.tests.annotations.Name;
import com.hazelcast.stabilizer.tests.annotations.Performance;
import com.hazelcast.stabilizer.tests.annotations.Receive;
import com.hazelcast.stabilizer.tests.annotations.Run;
import com.hazelcast.stabilizer.tests.annotations.Setup;
import com.hazelcast.stabilizer.tests.annotations.Verify;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class TestContainerTest {
    // =================== setup ========================

    @Test
    public void testRun() throws Throwable {
        DummyTest dummyTest = new DummyTest();
        TestContainer invoker = new TestContainer(dummyTest, new DummyTestContext(), new ProbesConfiguration());
        invoker.run();

        assertTrue(dummyTest.runCalled);
    }

    @Test(expected = IllegalTestException.class)
    public void runMissing() throws Throwable {
        RunMissingTest test = new RunMissingTest();
        new TestContainer(test, new DummyTestContext(), new ProbesConfiguration());
    }

    static class RunMissingTest {

        @Setup
        void setup(TestContext context) {
        }
    }

    // =================== setup ========================

    @Test
    public void testSetup() throws Throwable {
        DummyTestContext testContext = new DummyTestContext();
        DummyTest test = new DummyTest();
        TestContainer invoker = new TestContainer(test, testContext, new ProbesConfiguration());
        invoker.run();
        invoker.setup();

        assertTrue(test.setupCalled);
        assertSame(testContext, test.context);
    }


    // =================== local verify ========================

    @Test
    public void localVerify() throws Throwable {
        DummyTestContext testContext = new DummyTestContext();
        LocalVerifyTest test = new LocalVerifyTest();
        TestContainer invoker = new TestContainer(test, testContext, new ProbesConfiguration());
        invoker.localVerify();

        assertTrue(test.localVerifyCalled);
    }

    @Test
    public void localProbeInjected() throws Throwable {
        DummyTestContext testContext = new DummyTestContext();
        DummyTest test = new DummyTest();
        TestContainer invoker = new TestContainer(test, testContext, new ProbesConfiguration());
        invoker.setup();

        assertNotNull(test.simpleProbe);
    }

    @Test
    public void localProbe_explicit_name_set_via_annotation() throws Throwable {
        DummyTestContext testContext = new DummyTestContext();
        DummyTest test = new DummyTest();
        TestContainer invoker = new TestContainer(test, testContext, new ProbesConfiguration());
        Map probeResults = invoker.getProbeResults();
        assertTrue(probeResults.keySet().contains("explicitProbeName"));
    }

    @Test
    public void localProbe_implicit_name() throws Throwable {
        DummyTestContext testContext = new DummyTestContext();
        DummyTest test = new DummyTest();
        TestContainer invoker = new TestContainer(test, testContext, new ProbesConfiguration());
        Map probeResults = invoker.getProbeResults();
        assertTrue(probeResults.keySet().contains("Probe2"));
    }


    @Test
    public void testMessageReceiver() throws Throwable {
        DummyTestContext testContext = new DummyTestContext();
        LocalVerifyTest test = new LocalVerifyTest();
        TestContainer invoker = new TestContainer(test, testContext, new ProbesConfiguration());
        Message message = Mockito.mock(Message.class);
        invoker.sendMessage(message);

        assertEquals(message, test.messagePassed);
    }

    static class LocalVerifyTest {
        boolean localVerifyCalled;
        Message messagePassed;

        @Verify(global = false)
        void verify() {
            localVerifyCalled = true;
        }

        @Setup
        void setup(TestContext testContext) {

        }

        @Run
        void run() {

        }

        @Receive
        public void receive(Message message) {
            messagePassed = message;
        }
    }

    // =================== global verify ========================

    @Test
    public void globalVerify() throws Throwable {
        DummyTestContext testContext = new DummyTestContext();
        GlobalVerifyTest test = new GlobalVerifyTest();
        TestContainer invoker = new TestContainer(test, testContext, new ProbesConfiguration());
        invoker.globalVerify();

        assertTrue(test.globalVerifyCalled);
    }

    static class GlobalVerifyTest {
        boolean globalVerifyCalled;

        @Verify(global = true)
        void verify() {
            globalVerifyCalled = true;
        }

        @Setup
        void setup(TestContext testContext) {

        }

        @Run
        void run() {

        }
    }

    // =================== performance ========================

    @Test
    public void performance() throws Throwable {
        DummyTestContext testContext = new DummyTestContext();
        PerformanceTest test = new PerformanceTest();
        TestContainer invoker = new TestContainer(test, testContext, new ProbesConfiguration());
        long count = invoker.getOperationCount();

        assertEquals(20, count);
    }

    static class PerformanceTest {

        @Performance
        public long getCount() {
            return 20;
        }

        @Run
        void run() {

        }
    }

    static class DummyTest {
        boolean runCalled;
        boolean setupCalled;
        TestContext context;
        SimpleProbe simpleProbe;

        @Setup
        void setup(TestContext context, @Name("explicitProbeName") SimpleProbe probe1, SimpleProbe probe2) {
            this.context = context;
            this.setupCalled = true;
            this.simpleProbe = probe1;
        }

        @Run
        void run() {
            runCalled = true;
        }
    }

    static class DummyTestContext implements TestContext {
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
            return false;
        }

        @Override
        public void stop() {
            throw new UnsupportedOperationException("Not implemented");
        }
    }
}
