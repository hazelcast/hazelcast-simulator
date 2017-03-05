package com.hazelcast.simulator.utils;

import com.hazelcast.simulator.probes.Probe;
import com.hazelcast.simulator.test.annotations.InjectProbe;
import com.hazelcast.simulator.worker.metronome.Metronome;
import org.junit.Test;

import java.lang.reflect.Field;

import static com.hazelcast.simulator.utils.AnnotationReflectionUtils.getProbeName;
import static com.hazelcast.simulator.utils.AnnotationReflectionUtils.isPartOfTotalThroughput;
import static com.hazelcast.simulator.utils.ReflectionUtils.getField;
import static com.hazelcast.simulator.utils.ReflectionUtils.invokePrivateConstructor;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

public class AnnotationReflectionUtilsTest {

    @Test
    public void testConstructor() throws Exception {
        invokePrivateConstructor(AnnotationReflectionUtils.class);
    }

    @Test
    public void testGetProbeName_withAnnotation() {
        Field field = getField(AnnotationTestClass.class, "namedProbe", Probe.class);
        assertEquals("testName", getProbeName(field));
    }

    @Test
    public void testGetProbeName_withAnnotation_default() {
        Field field = getField(AnnotationTestClass.class, "defaultValueProbe", Probe.class);
        assertEquals("defaultValueProbe", getProbeName(field));
    }

    @Test
    public void testGetProbeName_noAnnotation() {
        Field field = getField(AnnotationTestClass.class, "notAnnotatedProbe", Probe.class);
        assertEquals("notAnnotatedProbe", getProbeName(field));
    }

    @Test
    public void testGetProbeName_noFieldFound() {
        Field field = getField(AnnotationTestClass.class, "notFound", Probe.class);
        assertNull(getProbeName(field));
    }

    @Test
    public void testIsThroughputProbe_withAnnotation() {
        Field field = getField(AnnotationTestClass.class, "throughputProbe", Probe.class);
        assertTrue(isPartOfTotalThroughput(field));
    }

    @Test
    public void testIsThroughputProbe_withAnnotation_defaultValue() {
        Field field = getField(AnnotationTestClass.class, "defaultValueProbe", Probe.class);
        assertFalse(isPartOfTotalThroughput(field));
    }

    @Test
    public void testIsThroughputProbe_noAnnotation() {
        Field field = getField(AnnotationTestClass.class, "notAnnotatedProbe", Probe.class);
        assertFalse(isPartOfTotalThroughput(field));
    }

    @Test
    public void testIsThroughputProbe_noFieldFound() {
        Field field = getField(AnnotationTestClass.class, "notFound", Probe.class);
        assertFalse(isPartOfTotalThroughput(field));
    }

    @SuppressWarnings("unused")
    private static class AnnotationTestClass {

        @InjectProbe(name = "testName")
        private Probe namedProbe;

        @InjectProbe(useForThroughput = true)
        private Probe throughputProbe;

        @InjectProbe
        private Probe defaultValueProbe;

        private Probe notAnnotatedProbe;

        private Metronome notAnnotatedMetronome;
    }
}
