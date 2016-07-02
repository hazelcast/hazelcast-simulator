package com.hazelcast.simulator.utils;

import com.hazelcast.simulator.probes.Probe;
import com.hazelcast.simulator.test.annotations.InjectMetronome;
import com.hazelcast.simulator.test.annotations.InjectProbe;
import com.hazelcast.simulator.test.annotations.Run;
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.test.annotations.Warmup;
import com.hazelcast.simulator.worker.metronome.Metronome;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

import static com.hazelcast.simulator.utils.AnnotationReflectionUtils.ALWAYS_FILTER;
import static com.hazelcast.simulator.utils.AnnotationReflectionUtils.getAtMostOneMethodWithoutArgs;
import static com.hazelcast.simulator.utils.AnnotationReflectionUtils.getAtMostOneVoidMethod;
import static com.hazelcast.simulator.utils.AnnotationReflectionUtils.getAtMostOneVoidMethodWithoutArgs;
import static com.hazelcast.simulator.utils.AnnotationReflectionUtils.getMetronomeIntervalMillis;
import static com.hazelcast.simulator.utils.AnnotationReflectionUtils.getMetronomeType;
import static com.hazelcast.simulator.utils.AnnotationReflectionUtils.getProbeName;
import static com.hazelcast.simulator.utils.AnnotationReflectionUtils.isPartOfTotalThroughput;
import static com.hazelcast.simulator.utils.ReflectionUtils.getField;
import static com.hazelcast.simulator.utils.ReflectionUtils.invokePrivateConstructor;
import static com.hazelcast.simulator.worker.metronome.MetronomeType.BUSY_SPINNING;
import static com.hazelcast.simulator.worker.metronome.MetronomeType.SLEEPING;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

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

    @Test
    public void testGetMetronomeIntervalMillis_withAnnotation() {
        Field field = getField(AnnotationTestClass.class, "configuredMetronome", Metronome.class);
        assertEquals(23, getMetronomeIntervalMillis(field, 42));
    }

    @Test
    public void testGetMetronomeIntervalMillis_withAnnotation_default() {
        Field field = getField(AnnotationTestClass.class, "defaultValueMetronome", Metronome.class);
        assertEquals(42, getMetronomeIntervalMillis(field, 42));
    }

    @Test
    public void testGetMetronomeIntervalMillis_noAnnotation() {
        Field field = getField(AnnotationTestClass.class, "notAnnotatedMetronome", Metronome.class);
        assertEquals(42, getMetronomeIntervalMillis(field, 42));
    }

    @Test
    public void testGetMetronomeIntervalMillis_noFieldFound() {
        Field field = getField(AnnotationTestClass.class, "notFound", Metronome.class);
        assertEquals(42, getMetronomeIntervalMillis(field, 42));
    }

    @Test
    public void testGetMetronomeType_withAnnotation() {
        Field field = getField(AnnotationTestClass.class, "configuredMetronome", Metronome.class);
        assertEquals(BUSY_SPINNING, getMetronomeType(field, SLEEPING));
    }

    @Test
    public void testGetMetronomeType_withAnnotation_default() {
        Field field = getField(AnnotationTestClass.class, "defaultValueMetronome", Metronome.class);
        assertEquals(SLEEPING, getMetronomeType(field, SLEEPING));
    }

    @Test
    public void testGetMetronomeType_noAnnotation() {
        Field field = getField(AnnotationTestClass.class, "notAnnotatedMetronome", Metronome.class);
        assertEquals(SLEEPING, getMetronomeType(field, SLEEPING));
    }

    @Test
    public void testGetMetronomeType_noFieldFound() {
        Field field = getField(AnnotationTestClass.class, "notFound", Metronome.class);
        assertEquals(SLEEPING, getMetronomeType(field, SLEEPING));
    }

    @Test
    public void testGetAtMostOneVoidMethodSkipArgsCheck() {
        Method method = getAtMostOneVoidMethod(AnnotationTestClass.class, Warmup.class);
        assertEquals("voidMethod", method.getName());
    }

    @Test
    public void testGetAtMostOneVoidMethodWithoutArgs() {
        Method method = getAtMostOneVoidMethodWithoutArgs(AnnotationTestClass.class, Warmup.class);
        assertEquals("voidMethod", method.getName());
    }

    @Test
    public void testGetAtMostOneVoidMethodWithoutArgs_AnnotationFilter() {
        Method method = getAtMostOneVoidMethodWithoutArgs(AnnotationTestClass.class, Warmup.class, ALWAYS_FILTER);
        assertEquals("voidMethod", method.getName());
    }

    @Test
    public void testGetAtMostOneMethodWithoutArgs() {
        Method method = getAtMostOneMethodWithoutArgs(AnnotationTestClass.class, Teardown.class, String.class);
        assertEquals("stringMethod", method.getName());
    }

    @Test
    public void testGetAtMostOneMethodWithoutArgs_nothingFound() {
        Method method = getAtMostOneMethodWithoutArgs(AnnotationTestClass.class, Verify.class, Long.class);
        assertNull(method);
    }

    @Test(expected = ReflectionException.class)
    public void testGetAtMostOneVoidMethodWithoutArgs_multipleMethodsFound() {
        getAtMostOneVoidMethodWithoutArgs(AnnotationTestClass.class, Run.class);
    }

    @Test(expected = ReflectionException.class)
    public void testGetAtMostOneVoidMethodWithoutArgs_staticMethodsFound() {
        getAtMostOneVoidMethodWithoutArgs(AnnotationTestClass.class, RunWithWorker.class);
    }

    @Test(expected = ReflectionException.class)
    public void testGetAtMostOneVoidMethodWithoutArgs_wrongReturnTypeArgsFound() {
        getAtMostOneMethodWithoutArgs(AnnotationTestClass.class, Warmup.class, String.class);
    }

    @Test(expected = ReflectionException.class)
    public void testGetAtMostOneVoidMethodWithoutArgs_methodsWithArgsFound() {
        getAtMostOneVoidMethodWithoutArgs(AnnotationTestClass.class, Setup.class);
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

        @InjectMetronome(intervalMillis = 23, type = BUSY_SPINNING)
        private Metronome configuredMetronome;

        @InjectMetronome
        private Metronome defaultValueMetronome;

        private Metronome notAnnotatedMetronome;

        @Setup
        private void hasArguments(String ignored) {
        }

        @Teardown
        private String stringMethod() {
            return null;
        }

        @Warmup
        private void voidMethod() {
        }

        @Run
        private void multipleMethod1() {
        }

        @Run
        private void multipleMethod2() {
        }

        @RunWithWorker
        private static void staticMethod() {
        }
    }
}
