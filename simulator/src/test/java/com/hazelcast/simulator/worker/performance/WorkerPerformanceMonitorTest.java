package com.hazelcast.simulator.worker.performance;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.simulator.protocol.connector.ServerConnector;
import com.hazelcast.simulator.protocol.core.AddressLevel;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.operation.PerformanceStateOperation;
import com.hazelcast.simulator.protocol.operation.TestHistogramOperation;
import com.hazelcast.simulator.test.TestCase;
import com.hazelcast.simulator.test.TestContainer;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.TestPhase;
import com.hazelcast.simulator.tests.PerformanceMonitorProbeTest;
import com.hazelcast.simulator.tests.PerformanceMonitorTest;
import com.hazelcast.simulator.tests.SuccessTest;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.simulator.protocol.core.SimulatorAddress.COORDINATOR;
import static com.hazelcast.simulator.utils.CommonUtils.joinThread;
import static com.hazelcast.simulator.utils.CommonUtils.sleepMillis;
import static com.hazelcast.simulator.utils.EmptyStatement.ignore;
import static com.hazelcast.simulator.utils.FileUtils.deleteQuiet;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class WorkerPerformanceMonitorTest {

    private static final String TEST_NAME = "WorkerPerformanceMonitorTest";

    private final ConcurrentMap<String, TestContainer> tests = new ConcurrentHashMap<String, TestContainer>();

    private ServerConnector serverConnector;
    private WorkerPerformanceMonitor performanceMonitor;

    @Before
    public void setUp() {
        SimulatorAddress workerAddress = new SimulatorAddress(AddressLevel.WORKER, 1, 1, 0);

        serverConnector = mock(ServerConnector.class);
        when(serverConnector.getAddress()).thenReturn(workerAddress);

        performanceMonitor = new WorkerPerformanceMonitor(serverConnector, tests.values(), 100, TimeUnit.MILLISECONDS);
    }

    @After
    public void tearDown() {
        performanceMonitor.shutdown();
    }

    @AfterClass
    public static void cleanUp() {
        deleteQuiet("performance.csv");
        deleteQuiet("performance-" + TEST_NAME + ".csv");
        deleteQuiet(TEST_NAME + "-workerProbe.hdr");
        deleteQuiet(TEST_NAME + "-aggregated.hdr");
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
        addTest(new PerformanceMonitorTest());

        performanceMonitor.start();

        verifyNoMoreInteractions(serverConnector);
    }

    @Test
    public void test_whenTestWithProbeWhichIsRunning_thenSendPerformanceStates() {
        performanceMonitor.start();
        sleepMillis(300);

        PerformanceMonitorProbeTest test = new PerformanceMonitorProbeTest();
        addTest(test);

        Thread testRunnerThread = new TestRunnerThread();
        testRunnerThread.start();

        test.recordValue(MICROSECONDS.toNanos(500));
        sleepMillis(200);

        test.recordValue(MICROSECONDS.toNanos(200));
        test.recordValue(MICROSECONDS.toNanos(300));
        sleepMillis(200);

        test.stopTest();
        joinThread(testRunnerThread);

        performanceMonitor.shutdown();
        verifyServerConnector();
    }

    @Test
    public void test_whenTestWithProbeWhichIsRunningWithDelay_thenSendPerformanceStates() {
        PerformanceMonitorProbeTest test = new PerformanceMonitorProbeTest();
        addTest(test, 200);

        performanceMonitor.start();

        Thread testRunnerThread = new TestRunnerThread();
        testRunnerThread.start();

        test.stopTest();
        joinThread(testRunnerThread);

        performanceMonitor.shutdown();
        verifyServerConnector();
    }

    @Test
    public void test_whenTestWithProbeWhichIsNotRunningAnymore_thenDoNothing() throws Exception {
        addTest(new PerformanceMonitorTest());
        tests.get(TEST_NAME).invoke(TestPhase.RUN);

        performanceMonitor.start();

        verifyNoMoreInteractions(serverConnector);
    }

    private void addTest(Object test) {
        addTest(test, 0);
    }

    private void addTest(Object test, int delayMillis) {
        TestCase testCase = new TestCase(TEST_NAME);
        TestContainer testContainer = new TestContainer(new DelayTestContext(delayMillis), test, testCase);
        tests.put(TEST_NAME, testContainer);
    }

    private void verifyServerConnector() {
        verify(serverConnector, atLeastOnce()).submit(eq(COORDINATOR), any(PerformanceStateOperation.class));
        verify(serverConnector, times(1)).write(eq(COORDINATOR), any(TestHistogramOperation.class));
        verifyNoMoreInteractions(serverConnector);
    }

    private static class DelayTestContext implements TestContext {

        private final int delayMillis;

        DelayTestContext(int delayMillis) {
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
            return LOCALHOST;
        }

        @Override
        public boolean isStopped() {
            return true;
        }

        @Override
        public void stop() {
        }
    }

    private class TestRunnerThread extends Thread {
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
