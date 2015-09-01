package com.hazelcast.simulator.probes.probes.impl;

import com.hazelcast.simulator.probes.probes.IntervalProbe;
import com.hazelcast.simulator.probes.probes.ProbesConfiguration;
import com.hazelcast.simulator.probes.probes.ProbesType;
import com.hazelcast.simulator.probes.probes.SimpleProbe;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static com.hazelcast.simulator.probes.probes.Probes.createConcurrentProbe;
import static com.hazelcast.simulator.utils.ReflectionUtils.getObjectFromField;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ConcurrentProbeTest {

    private ProbesConfiguration config = new ProbesConfiguration();
    private ConcurrentProbe<?, ?> concurrentProbe;

    @Before
    @SuppressWarnings("unchecked")
    public void setUp() {
        config.addConfig("throughput", ProbesType.THROUGHPUT.getName());
        config.addConfig("latency", ProbesType.MAX_LATENCY.getName());
        concurrentProbe = (ConcurrentProbe) createConcurrentProbe("throughput", SimpleProbe.class, config);
    }

    @Test
    public void testThreadLocalProbesForUniqueness() throws Exception {
        ProbeTester probeTester1 = new ProbeTester(concurrentProbe);
        ProbeTester probeTester2 = new ProbeTester(concurrentProbe);

        probeTester1.start();
        probeTester2.start();

        probeTester1.join();
        probeTester2.join();

        SimpleProbe threadLocalProbe1 = probeTester1.threadLocalProbe;
        SimpleProbe threadLocalProbe2 = probeTester2.threadLocalProbe;

        assertNotEquals(concurrentProbe, threadLocalProbe1);
        assertNotEquals(concurrentProbe, threadLocalProbe2);
        assertNotEquals(threadLocalProbe1, threadLocalProbe2);

        assertTrue(threadLocalProbe1 instanceof OperationsPerSecProbe);
        assertTrue(threadLocalProbe2 instanceof OperationsPerSecProbe);
    }

    @Test
    public void testInvocationCountOnSimpleProbe() throws Exception {
        ProbeTester probeTester1 = new ProbeTester(concurrentProbe);
        ProbeTester probeTester2 = new ProbeTester(concurrentProbe);

        probeTester1.start();
        probeTester2.start();

        probeTester1.join();
        probeTester2.join();

        assertEquals(2, concurrentProbe.getInvocationCount());
        assertNotNull(concurrentProbe.getResult());
        assertEquals(2, concurrentProbe.probeMapSize());
    }

    @Test
    public void testInvocationCountOnSimpleProbe_disableSomeProbes() throws Exception {
        ProbeTester probeTester1 = new ProbeTester(concurrentProbe);
        ProbeTester probeTester2 = new ProbeTester(concurrentProbe);
        ProbeDisableTester probeTester3 = new ProbeDisableTester(concurrentProbe);
        ProbeDisableTester probeTester4 = new ProbeDisableTester(concurrentProbe);

        probeTester1.start();
        probeTester2.start();
        probeTester3.start();
        probeTester4.start();

        probeTester1.join();
        probeTester2.join();
        probeTester3.join();
        probeTester4.join();

        assertFalse(probeTester1.threadLocalProbe.isDisabled());
        assertFalse(probeTester2.threadLocalProbe.isDisabled());
        assertTrue(probeTester3.threadLocalProbe.isDisabled());
        assertTrue(probeTester4.threadLocalProbe.isDisabled());
        assertFalse(concurrentProbe.isDisabled());

        assertEquals(2, concurrentProbe.getInvocationCount());
        assertNotNull(concurrentProbe.getResult());
        assertEquals(4, concurrentProbe.probeMapSize());
    }

    @Test
    public void testInvocationCountOnSimpleProbe_disableAllProbes() throws Exception {
        ProbeDisableTester probeTester1 = new ProbeDisableTester(concurrentProbe);
        ProbeDisableTester probeTester2 = new ProbeDisableTester(concurrentProbe);
        ProbeDisableTester probeTester3 = new ProbeDisableTester(concurrentProbe);

        probeTester1.start();
        probeTester2.start();
        probeTester3.start();

        probeTester1.join();
        probeTester2.join();
        probeTester3.join();

        assertTrue(probeTester1.threadLocalProbe.isDisabled());
        assertTrue(probeTester2.threadLocalProbe.isDisabled());
        assertTrue(probeTester3.threadLocalProbe.isDisabled());
        assertTrue(concurrentProbe.isDisabled());

        assertEquals(0, concurrentProbe.getInvocationCount());
        assertNull(concurrentProbe.getResult());
        assertEquals(3, concurrentProbe.probeMapSize());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testInvocationCountOnIntervalProbe() throws Exception {
        concurrentProbe = (ConcurrentProbe) createConcurrentProbe("latency", IntervalProbe.class, config);

        ProbeTester probeTester1 = new ProbeTester(concurrentProbe);
        ProbeTester probeTester2 = new ProbeTester(concurrentProbe);

        probeTester1.start();
        probeTester2.start();

        probeTester1.join();
        probeTester2.join();

        assertEquals(2, concurrentProbe.getInvocationCount());
        assertNotNull(concurrentProbe.getResult());
        assertEquals(2, concurrentProbe.probeMapSize());
    }

    @Test
    public void testInvocationCountOnSimpleProbeWithSetValues() throws Exception {
        ProbeSetValuesTester probeTester1 = new ProbeSetValuesTester(concurrentProbe);
        ProbeSetValuesTester probeTester2 = new ProbeSetValuesTester(concurrentProbe);

        probeTester1.start();
        probeTester2.start();

        probeTester1.join();
        probeTester2.join();

        assertEquals(10, concurrentProbe.getInvocationCount());
        assertNotNull(concurrentProbe.getResult());
        assertEquals(2, concurrentProbe.probeMapSize());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testInvocationCountOnIntervalProbeWithRecordValue() throws Exception {
        concurrentProbe = (ConcurrentProbe) createConcurrentProbe("latency", IntervalProbe.class, config);

        ProbeRecordValueTester probeTester1 = new ProbeRecordValueTester(concurrentProbe);
        ProbeRecordValueTester probeTester2 = new ProbeRecordValueTester(concurrentProbe);

        probeTester1.start();
        probeTester2.start();

        probeTester1.join();
        probeTester2.join();

        assertEquals(2, concurrentProbe.getInvocationCount());
        assertNotNull(concurrentProbe.getResult());
        assertEquals(2, concurrentProbe.probeMapSize());
    }

    @Test
    public void testEmptyResult() {
        assertNull(concurrentProbe.getResult());
    }

    @Test
    public void testCombinedResult() throws Exception {
        ProbeTester probeTester1 = new ProbeTester(concurrentProbe);
        ProbeTester probeTester2 = new ProbeTester(concurrentProbe);

        long started = System.currentTimeMillis();
        concurrentProbe.startProbing(started);

        probeTester1.start();
        probeTester2.start();

        probeTester1.join();
        probeTester2.join();

        concurrentProbe.stopProbing(started + TimeUnit.SECONDS.toMillis(1));

        OperationsPerSecResult result1 = (OperationsPerSecResult) probeTester1.threadLocalProbe.getResult();
        Long invocations1 = getObjectFromField(result1, "invocations");
        Double operationsPerSecond1 = getObjectFromField(result1, "operationsPerSecond");

        OperationsPerSecResult result2 = (OperationsPerSecResult) probeTester2.threadLocalProbe.getResult();
        Long invocations2 = getObjectFromField(result2, "invocations");
        Double operationsPerSecond2 = getObjectFromField(result2, "operationsPerSecond");

        OperationsPerSecResult combinedResult = (OperationsPerSecResult) concurrentProbe.getResult();
        Long combinedInvocations = getObjectFromField(combinedResult, "invocations");
        Double combinedOperationsPerSecond = getObjectFromField(combinedResult, "operationsPerSecond");

        assertEquals(1, invocations1.longValue());
        assertEquals(1, invocations2.longValue());
        assertEquals(2, combinedInvocations.longValue());

        assertEquals(1.0, operationsPerSecond1, 0.0001);
        assertEquals(1.0, operationsPerSecond2, 0.0001);
        assertEquals(2.0, combinedOperationsPerSecond, 0.0001);
    }

    private static class ProbeTester extends Thread {

        private final ConcurrentProbe probe;

        private SimpleProbe threadLocalProbe;

        public ProbeTester(ConcurrentProbe probe) {
            this.probe = probe;
        }

        @Override
        public void run() {
            threadLocalProbe = probe.getProbe();
            probe.started();
            probe.done();
        }
    }

    private static class ProbeDisableTester extends Thread {

        private final ConcurrentProbe probe;

        private SimpleProbe threadLocalProbe;

        public ProbeDisableTester(ConcurrentProbe probe) {
            this.probe = probe;
        }

        @Override
        public void run() {
            threadLocalProbe = probe.getProbe();
            assertFalse(probe.isDisabled());
            probe.disable();
        }
    }

    private static class ProbeSetValuesTester extends Thread {

        private final ConcurrentProbe probe;

        public ProbeSetValuesTester(ConcurrentProbe probe) {
            this.probe = probe;
        }

        @Override
        public void run() {
            probe.setValues(1000, 5);
        }
    }

    private static class ProbeRecordValueTester extends Thread {

        private final ConcurrentProbe probe;

        public ProbeRecordValueTester(ConcurrentProbe probe) {
            this.probe = probe;
        }

        @Override
        public void run() {
            probe.recordValue(TimeUnit.MICROSECONDS.toNanos(40));
        }
    }
}
