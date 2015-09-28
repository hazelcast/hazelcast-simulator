package com.hazelcast.simulator.probes.probes.impl;

import com.hazelcast.simulator.probes.probes.Probe;
import com.hazelcast.simulator.probes.probes.Result;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ConcurrentProbeTest {

    private ConcurrentProbe concurrentProbe = new ConcurrentProbe();

    @Test
    public void testThreadLocalProbesForUniqueness() throws Exception {
        ProbeTester probeTester1 = new ProbeTester(concurrentProbe);
        ProbeTester probeTester2 = new ProbeTester(concurrentProbe);

        probeTester1.start();
        probeTester2.start();

        probeTester1.join();
        probeTester2.join();

        Probe threadLocalProbe1 = probeTester1.threadLocalProbe;
        Probe threadLocalProbe2 = probeTester2.threadLocalProbe;

        assertNotEquals(concurrentProbe, threadLocalProbe1);
        assertNotEquals(concurrentProbe, threadLocalProbe2);
        assertNotEquals(threadLocalProbe1, threadLocalProbe2);

        assertTrue(threadLocalProbe1 instanceof ProbeImpl);
        assertTrue(threadLocalProbe2 instanceof ProbeImpl);
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
    public void testInvocationCountOnIntervalProbe() throws Exception {
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
    public void testInvocationCountOnIntervalProbeWithRecordValue() throws Exception {
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

        Result result1 = probeTester1.threadLocalProbe.getResult();
        Long invocations1 = result1.getInvocationCount();
        Double throughput1 = result1.getThroughput();

        Result result2 = probeTester2.threadLocalProbe.getResult();
        Long invocations2 = result2.getInvocationCount();
        Double throughput2 = result2.getThroughput();

        Result combinedResult = concurrentProbe.getResult();
        Long combinedInvocations = combinedResult.getInvocationCount();
        Double combinedThroughput = combinedResult.getThroughput();

        assertEquals(1, invocations1.longValue());
        assertEquals(1, invocations2.longValue());
        assertEquals(2, combinedInvocations.longValue());

        assertEquals(1.0, throughput1, 0.0001);
        assertEquals(1.0, throughput2, 0.0001);
        assertEquals(2.0, combinedThroughput, 0.0001);
    }

    private static class ProbeTester extends Thread {

        private final ConcurrentProbe probe;

        private Probe threadLocalProbe;

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

        private Probe threadLocalProbe;

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
