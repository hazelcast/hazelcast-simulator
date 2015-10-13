package com.hazelcast.simulator.worker.performance;

import com.hazelcast.simulator.protocol.connector.ServerConnector;
import com.hazelcast.simulator.protocol.core.AddressLevel;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.operation.LogOperation;
import com.hazelcast.simulator.test.TestPhase;
import com.hazelcast.simulator.tests.PerformanceMonitorProbeTest;
import com.hazelcast.simulator.tests.PerformanceMonitorTest;
import com.hazelcast.simulator.tests.SuccessTest;
import com.hazelcast.simulator.utils.EmptyStatement;
import com.hazelcast.simulator.worker.DummyTestContext;
import com.hazelcast.simulator.worker.TestContainer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.mockito.verification.VerificationWithTimeout;

import java.io.File;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.simulator.utils.FileUtils.deleteQuiet;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class WorkerPerformanceMonitorTest {

    private static final VerificationWithTimeout VERIFY_TIMEOUT = timeout(TimeUnit.SECONDS.toMillis(1));

    private final ConcurrentMap<String, TestContainer> tests = new ConcurrentHashMap<String, TestContainer>();
    private final DummyTestContext testContext = new DummyTestContext();

    private ServerConnector serverConnector;
    private WorkerPerformanceMonitor performanceMonitor;

    @Before
    public void setUp() {
        SimulatorAddress workerAddress = new SimulatorAddress(AddressLevel.WORKER, 1, 1, 0);

        serverConnector = mock(ServerConnector.class);
        when(serverConnector.getAddress()).thenReturn(workerAddress);

        performanceMonitor = new WorkerPerformanceMonitor(serverConnector, tests.values(), 1);
    }

    @After
    public void tearDown() {
        performanceMonitor.shutdown();
    }

    @AfterClass
    public static void cleanUp() {
        deleteQuiet(new File("throughput.txt"));
        deleteQuiet(new File("throughput-DummyTestContext.txt"));
        deleteQuiet(new File("latency-DummyTestContext-DummyTestContextWorkerProbe.txt"));
        deleteQuiet(new File("latency-DummyTestContext-aggregated.txt"));
    }

    @Test
    public void test_startTwice() {
        assertTrue(performanceMonitor.start());
        assertFalse(performanceMonitor.start());
    }

    @Test
    public void test_noTests() {
        assertTrue(performanceMonitor.start());
    }

    @Test
    public void test_testWithoutPerformanceAnnotation() {
        addTest(new SuccessTest());

        assertTrue(performanceMonitor.start());

        verifyServerConnector();
    }

    @Test
    public void test_testWithPerformanceAnnotation() {
        addTest(new PerformanceMonitorTest());

        assertTrue(performanceMonitor.start());

        verifyServerConnector();
    }

    @Test
    public void test_testWithProbe() throws Exception {
        PerformanceMonitorProbeTest test = new PerformanceMonitorProbeTest();
        addTest(test);

        Thread thread = new Thread() {
            @Override
            public void run() {
                try {
                    tests.get("test").invoke(TestPhase.RUN);
                } catch (Exception e) {
                    EmptyStatement.ignore(e);
                }
            }
        };
        thread.start();

        test.recordValue(TimeUnit.MICROSECONDS.toNanos(500));
        test.recordValue(TimeUnit.MICROSECONDS.toNanos(200));
        test.recordValue(TimeUnit.MICROSECONDS.toNanos(300));

        assertTrue(performanceMonitor.start());

        verifyServerConnector();

        test.stopTest();
        thread.join();
    }

    @Test
    public void test_testAfterRun() throws Exception {
        addTest(new PerformanceMonitorTest());
        tests.get("test").invoke(TestPhase.RUN);

        assertTrue(performanceMonitor.start());

        verifyServerConnector();
    }

    private void addTest(Object test) {
        TestContainer testContainer = new TestContainer(test, testContext, null);
        tests.put("test", testContainer);
    }

    private void verifyServerConnector() {
        verify(serverConnector, VERIFY_TIMEOUT.atLeastOnce()).submit(eq(SimulatorAddress.COORDINATOR), any(LogOperation.class));
        verify(serverConnector, atLeastOnce()).getAddress();
        verifyNoMoreInteractions(serverConnector);
    }
}
