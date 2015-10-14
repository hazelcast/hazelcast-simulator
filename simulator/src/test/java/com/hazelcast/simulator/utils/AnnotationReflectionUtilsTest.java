package com.hazelcast.simulator.utils;

import com.hazelcast.simulator.probes.Probe;
import com.hazelcast.simulator.test.annotations.Run;
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.SimulatorProbe;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.test.annotations.Warmup;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

import static com.hazelcast.simulator.utils.AnnotationReflectionUtils.ALWAYS_FILTER;
import static com.hazelcast.simulator.utils.AnnotationReflectionUtils.getAtMostOneMethodWithoutArgs;
import static com.hazelcast.simulator.utils.AnnotationReflectionUtils.getAtMostOneVoidMethodSkipArgsCheck;
import static com.hazelcast.simulator.utils.AnnotationReflectionUtils.getAtMostOneVoidMethodWithoutArgs;
import static com.hazelcast.simulator.utils.AnnotationReflectionUtils.getProbeName;
import static com.hazelcast.simulator.utils.AnnotationReflectionUtils.isThroughputProbe;
import static com.hazelcast.simulator.utils.ReflectionUtils.getField;
import static com.hazelcast.simulator.utils.ReflectionUtils.invokePrivateConstructor;
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
        assertTrue(isThroughputProbe(field));
    }

    @Test
    public void testIsThroughputProbe_withAnnotation_defaultValue() {
        Field field = getField(AnnotationTestClass.class, "defaultValueProbe", Probe.class);
        assertFalse(isThroughputProbe(field));
    }

    @Test
    public void testIsThroughputProbe_noAnnotation() {
        Field field = getField(AnnotationTestClass.class, "notAnnotatedProbe", Probe.class);
        assertFalse(isThroughputProbe(field));
    }

    @Test
    public void testIsThroughputProbe_noFieldFound() {
        Field field = getField(AnnotationTestClass.class, "notFound", Probe.class);
        assertFalse(isThroughputProbe(field));
    }

    @Test
    public void testGetAtMostOneVoidMethodSkipArgsCheck() {
        Method method = getAtMostOneVoidMethodSkipArgsCheck(AnnotationTestClass.class, Warmup.class);
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

        @SimulatorProbe(name = "testName")
        private Probe namedProbe;

        @SimulatorProbe(useForThroughput = true)
        private Probe throughputProbe;

        @SimulatorProbe
        private Probe defaultValueProbe;

        private Probe notAnnotatedProbe;

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
