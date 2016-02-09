package com.hazelcast.simulator.test;

import com.hazelcast.simulator.probes.Probe;
import com.hazelcast.simulator.test.annotations.InjectProbe;
import com.hazelcast.simulator.test.annotations.InjectTestContext;
import com.hazelcast.simulator.test.annotations.Run;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TestContainer_InjectProbeTest extends AbstractTestContainerTest {

    @Test
    public void testInjectProbe() throws Exception {
        ProbeTest test = new ProbeTest();
        testContainer = createTestContainer(test);

        assertNotNull(test.probe);
        assertFalse(test.probe.isThroughputProbe());
        assertTrue(testContainer.hasProbe("probe"));

        testContainer.invoke(TestPhase.RUN);
        Map<String, Probe> probeMap = testContainer.getProbeMap();
        assertTrue(probeMap.size() > 0);
        assertTrue(probeMap.containsKey("probe"));
    }

    @Test
    public void testInjectProbe_withName() {
        ProbeTest test = new ProbeTest();
        testContainer = createTestContainer(test);

        assertNotNull(test.namedProbe);
        assertTrue(testContainer.hasProbe("explicitProbeName"));
    }

    @Test
    public void testInjectProbe_withUseForThroughput() {
        ProbeTest test = new ProbeTest();
        testContainer = createTestContainer(test);

        assertNotNull(test.throughputProbe);
        assertTrue(test.throughputProbe.isThroughputProbe());
        assertTrue(testContainer.hasProbe("throughputProbe"));
    }

    @Test
    public void testInjectProbe_withoutAnnotation() {
        ProbeTest test = new ProbeTest();
        testContainer = createTestContainer(test);

        assertNull(test.notAnnotatedProbe);
    }

    private static class ProbeTest extends BaseTest {

        @InjectTestContext
        private TestContext context;

        @InjectProbe
        private Probe probe;

        @InjectProbe(name = "explicitProbeName")
        private Probe namedProbe;

        @InjectProbe(useForThroughput = true)
        private Probe throughputProbe;

        @SuppressWarnings("unused")
        private Probe notAnnotatedProbe;

        @Run
        public void run() {
            probe.started();
            probe.done();
        }
    }

    @Test(expected = IllegalTestException.class)
    public void testInjectProbe_withIllegalFieldType() {
        IllegalFieldTypeTest test = new IllegalFieldTypeTest();
        testContainer = createTestContainer(test);
    }

    private static class IllegalFieldTypeTest extends BaseTest {

        @InjectProbe
        private Object noProbeField;
    }

    @Test
    public void testInjectProbe_withDuplicateProbeName() {
        DuplicateProbeNameTest test = new DuplicateProbeNameTest();
        testContainer = createTestContainer(test);

        assertTrue(testContainer.hasProbe("sameProbeName"));
        assertEquals(test.sameProbeName, test.explicitSameProbeName);
        assertEquals(test.sameProbeName, test.anotherExplicitSameProbeName);
        assertEquals(test.explicitSameProbeName, test.anotherExplicitSameProbeName);
    }

    private static class DuplicateProbeNameTest extends BaseTest {

        @InjectProbe
        private Probe sameProbeName;

        @InjectProbe(name = "sameProbeName")
        private Probe explicitSameProbeName;

        @InjectProbe(name = "sameProbeName")
        private Probe anotherExplicitSameProbeName;
    }
}
