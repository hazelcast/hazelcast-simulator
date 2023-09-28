package com.hazelcast.simulator.worker.performance;

import com.hazelcast.simulator.common.TestCase;
import com.hazelcast.simulator.common.TestPhase;
import com.hazelcast.simulator.protocol.Server;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.tests.DummyTest;
import com.hazelcast.simulator.tests.SuccessTest;
import com.hazelcast.simulator.utils.AssertTask;
import com.hazelcast.simulator.worker.operations.PerformanceStatsOperation;
import com.hazelcast.simulator.worker.testcontainer.TestContainer;
import com.hazelcast.simulator.worker.testcontainer.TestContextImpl;
import com.hazelcast.simulator.worker.testcontainer.TestManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.simulator.TestEnvironmentUtils.setupFakeUserDir;
import static com.hazelcast.simulator.TestEnvironmentUtils.teardownFakeUserDir;
import static com.hazelcast.simulator.utils.CommonUtils.joinThread;
import static com.hazelcast.simulator.utils.CommonUtils.sleepMillis;
import static com.hazelcast.simulator.utils.EmptyStatement.ignore;
import static com.hazelcast.simulator.utils.TestUtils.assertTrueEventually;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class PerformanceMonitorTest {

    private static final String TEST_NAME = "WorkerPerformanceMonitorTest";

    private final ConcurrentMap<String, TestContainer> tests = new ConcurrentHashMap<String, TestContainer>();

    private Server server;
    private OperationsMonitor performanceMonitor;
    private TestManager containerManager;

    @Before
    public void before() {
        setupFakeUserDir();

        server = mock(Server.class);

        containerManager = mock(TestManager.class);
        when(containerManager.getContainers()).thenReturn(tests.values());

        performanceMonitor = new OperationsMonitor(server, containerManager, 1);
    }

    @After
    public void after() {
        performanceMonitor.close();

        teardownFakeUserDir();
    }

    @Test
    public void test_closeTwice() {
        performanceMonitor.start();

        performanceMonitor.close();
        performanceMonitor.close();
    }

    @Test(expected = IllegalThreadStateException.class)
    public void test_restartAfterStop() {
        performanceMonitor.start();

        performanceMonitor.close();

        performanceMonitor.start();
    }

    @Test
    public void test_whenTestWithoutProbe_thenDoNothing() {
        addTest(new SuccessTest());

        performanceMonitor.start();

        verifyNoMoreInteractions(server);
    }

    @Test
    public void test_whenTestWithProbeWhichIsNotRunning_thenDoNothing() {
        addTest(new DummyTest());

        performanceMonitor.start();

        verifyNoMoreInteractions(server);
    }

    @Test
    public void test_whenTestRunning_thenSendPerformanceStats() {
        performanceMonitor.start();
        sleepMillis(300);

        DummyTest test = new DummyTest();
        TestContext testContext = addTest(test);

        Thread runTestThread = new RunTestThread();
        runTestThread.start();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertPerfStatsSend();
            }
        });

        testContext.stop();
        joinThread(runTestThread);

        performanceMonitor.close();
    }

    private TestContext addTest(Object test) {
        return addTest(test, 0);
    }

    private DelayTestContext addTest(Object test, int delayMillis) {
        TestCase testCase = new TestCase(TEST_NAME);
        testCase.setProperty("threadCount", 1);
        DelayTestContext testContext = new DelayTestContext(delayMillis);
        TestContainer testContainer = new TestContainer(testContext, test, testCase);

        tests.put(TEST_NAME, testContainer);
        return testContext;
    }

    private void assertPerfStatsSend() {
        verify(server, atLeastOnce()).sendCoordinator(any(PerformanceStatsOperation.class));
        verifyNoMoreInteractions(server);
    }

    private static class DelayTestContext extends TestContextImpl {

        private final int delayMillis;
        private volatile boolean stopped = false;

        DelayTestContext(int delayMillis) {
            super(TEST_NAME, "localhost", mock(Server.class));
            this.delayMillis = delayMillis;
        }

        @Override
        public String getTestId() {
            sleepMillis(delayMillis);
            return TEST_NAME;
        }

        @Override
        public String getPublicIpAddress() {
            return "127.0.0.1";
        }

        @Override
        public boolean isStopped() {
            return stopped;
        }

        @Override
        public void stop() {
            stopped = true;
        }
    }

    private class RunTestThread extends Thread {
        @Override
        public void run() {
            try {
                tests.get(TEST_NAME).invoke(TestPhase.RUN);
            } catch (Exception e) {
                ignore(e);
            }
        }
    }
}
