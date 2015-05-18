package com.hazelcast.simulator.worker;

import com.hazelcast.simulator.probes.probes.ProbesConfiguration;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.tests.PerformanceMonitorTest;
import com.hazelcast.simulator.tests.SuccessTest;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.simulator.utils.CommonUtils.sleepMillis;
import static com.hazelcast.simulator.utils.CommonUtils.sleepSeconds;
import static com.hazelcast.simulator.utils.FileUtils.deleteQuiet;

public class WorkerPerformanceMonitorTest {

    private final ConcurrentMap<String, TestContainer<TestContext>> tests
            = new ConcurrentHashMap<String, TestContainer<TestContext>>();
    private final WorkerPerformanceMonitor monitor = new WorkerPerformanceMonitor(tests.values(), 1);
    private final DummyTestContext testContext = new DummyTestContext();
    private final ProbesConfiguration probesConfiguration = new ProbesConfiguration();

    @After
    public void tearDown() {
        monitor.stop();
    }

    @AfterClass
    public static void cleanUp() {
        sleepMillis(100);
        deleteQuiet(new File("performance.txt"));
        deleteQuiet(new File("performance-default.txt"));
    }

    @Test
    public void testStart() {
        monitor.start();
    }

    @Test
    public void testStart_twice() {
        monitor.start();
        monitor.start();
    }

    @Test
    public void testStart_runTestWithoutPerformanceAnnotation() {
        addTest(new SuccessTest());

        monitor.start();
        sleepSeconds(2);
    }

    @Test
    public void testStart_runTestWithPerformanceAnnotation() {
        addTest(new PerformanceMonitorTest());

        monitor.start();
        sleepSeconds(3);
    }

    private void addTest(Object test) {
        TestContainer<TestContext> testContainer = new TestContainer<TestContext>(test, testContext, probesConfiguration, null);
        tests.put("test", testContainer);
    }
}
