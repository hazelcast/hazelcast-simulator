package com.hazelcast.simulator.worker.performance;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.simulator.common.TestCase;
import com.hazelcast.simulator.common.TestPhase;
import com.hazelcast.simulator.protocol.connector.ServerConnector;
import com.hazelcast.simulator.protocol.connector.WorkerConnector;
import com.hazelcast.simulator.protocol.core.AddressLevel;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.operation.PerformanceStatsOperation;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.tests.DummyTest;
import com.hazelcast.simulator.tests.SuccessTest;
import com.hazelcast.simulator.utils.AssertTask;
import com.hazelcast.simulator.worker.testcontainer.TestContainer;
import com.hazelcast.simulator.worker.testcontainer.TestContextImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.simulator.TestEnvironmentUtils.setupFakeUserDir;
import static com.hazelcast.simulator.TestEnvironmentUtils.teardownFakeUserDir;
import static com.hazelcast.simulator.protocol.core.SimulatorAddress.COORDINATOR;
import static com.hazelcast.simulator.utils.CommonUtils.joinThread;
import static com.hazelcast.simulator.utils.CommonUtils.sleepMillis;
import static com.hazelcast.simulator.utils.EmptyStatement.ignore;
import static com.hazelcast.simulator.utils.TestUtils.assertTrueEventually;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class PerformanceMonitorTest {

    private static final String TEST_NAME = "WorkerPerformanceMonitorTest";

    private final ConcurrentMap<String, TestContainer> tests = new ConcurrentHashMap<String, TestContainer>();

    private ServerConnector serverConnector;
    private PerformanceMonitor performanceMonitor;

    @Before
    public void before() {
        setupFakeUserDir();

        SimulatorAddress workerAddress = new SimulatorAddress(AddressLevel.WORKER, 1, 1, 0);

        serverConnector = mock(ServerConnector.class);
        when(serverConnector.getAddress()).thenReturn(workerAddress);

        performanceMonitor = new PerformanceMonitor(serverConnector, tests.values(), 1);
    }

    @After
    public void after() {
        performanceMonitor.shutdown();

        teardownFakeUserDir();
    }

    @Test
    public void test_shutdownTwice() {
        performanceMonitor.start();

        performanceMonitor.shutdown();
        performanceMonitor.shutdown();
    }

    @Test(expected = IllegalThreadStateException.class)
    public void test_restartAfterStop() {
        performanceMonitor.start();

        performanceMonitor.shutdown();

        performanceMonitor.start();
    }

    @Test
    public void test_whenTestWithoutProbe_thenDoNothing() {
        addTest(new SuccessTest());

        performanceMonitor.start();

        verifyNoMoreInteractions(serverConnector);
    }

    @Test
    public void test_whenTestWithProbeWhichIsNotRunning_thenDoNothing() {
        addTest(new DummyTest());

        performanceMonitor.start();

        verifyNoMoreInteractions(serverConnector);
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

        performanceMonitor.shutdown();
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
        verify(serverConnector, atLeastOnce()).submit(eq(COORDINATOR), any(PerformanceStatsOperation.class));
        verifyNoMoreInteractions(serverConnector);
    }

    private static class DelayTestContext extends TestContextImpl {

        private final int delayMillis;
        private volatile boolean stopped = false;

        DelayTestContext(int delayMillis) {
            super(null, TEST_NAME, "localhost", mock(WorkerConnector.class));
            this.delayMillis = delayMillis;
        }

        @Override
        public HazelcastInstance getTargetInstance() {
            return null;
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
