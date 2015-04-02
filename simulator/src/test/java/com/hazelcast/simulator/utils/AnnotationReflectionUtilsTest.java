package com.hazelcast.simulator.utils;

import com.hazelcast.simulator.test.annotations.Name;
import com.hazelcast.simulator.test.annotations.Performance;
import com.hazelcast.simulator.test.annotations.Receive;
import com.hazelcast.simulator.test.annotations.Run;
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.Verify;
import org.junit.Test;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;

import static com.hazelcast.simulator.utils.AnnotationReflectionUtils.ALWAYS_FILTER;
import static com.hazelcast.simulator.utils.AnnotationReflectionUtils.getAtMostOneMethodWithoutArgs;
import static com.hazelcast.simulator.utils.AnnotationReflectionUtils.getAtMostOneVoidMethodSkipArgsCheck;
import static com.hazelcast.simulator.utils.AnnotationReflectionUtils.getAtMostOneVoidMethodWithoutArgs;
import static com.hazelcast.simulator.utils.AnnotationReflectionUtils.getValueFromNameAnnotation;
import static com.hazelcast.simulator.utils.AnnotationReflectionUtils.getValueFromNameAnnotations;
import static com.hazelcast.simulator.utils.ReflectionUtils.getField;
import static com.hazelcast.simulator.utils.ReflectionUtils.invokePrivateConstructor;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class AnnotationReflectionUtilsTest {

    @Test
    public void testConstructor() throws Exception {
        invokePrivateConstructor(AnnotationReflectionUtils.class);
    }

    @Test
    public void testGetValueFromNameAnnotation() {
        Field field = getField(AnnotationTestClass.class, "annotatedField", Object.class);

        String actual = getValueFromNameAnnotation(field);
        assertEquals("testName", actual);
    }

    @Test
    public void testGetValueFromNameAnnotation_noAnnotation() {
        Field field = getField(AnnotationTestClass.class, "notAnnotatedField", Object.class);

        String actual = getValueFromNameAnnotation(field);
        assertEquals("notAnnotatedField", actual);
    }

    @Test
    public void testGetValueFromNameAnnotations() throws Exception {
        Annotation[] annotations = new Annotation[1];
        annotations[0] = new TestAnnotation("testValue");

        String actual = getValueFromNameAnnotations(annotations, "notReturned");
        assertEquals("testValue", actual);
    }

    @Test
    public void testGetValueFromNameAnnotations_multipleAnnotations() throws Exception {
        Annotation[] annotations = new Annotation[2];
        annotations[0] = new TestAnnotation("firstValue");
        annotations[1] = new TestAnnotation("secondValue");

        String actual = getValueFromNameAnnotations(annotations, "notReturned");
        assertEquals("firstValue", actual);
    }

    @Test
    public void testGetValueFromNameAnnotations_defaultValue() throws Exception {
        Annotation[] annotations = new Annotation[2];
        annotations[0] = new TestAnnotation("notReturned1", Annotation.class);
        annotations[1] = new TestAnnotation("notReturned2", Annotation.class);

        String actual = getValueFromNameAnnotations(annotations, "defaultValue");
        assertEquals("defaultValue", actual);
    }

    @Test
    public void testGetAtMostOneVoidMethodSkipArgsCheck() {
        Method method = getAtMostOneVoidMethodSkipArgsCheck(AnnotationTestClass.class, Setup.class);
        assertEquals("voidMethod", method.getName());
    }

    @Test
    public void testGetAtMostOneVoidMethodWithoutArgs() {
        Method method = getAtMostOneVoidMethodWithoutArgs(AnnotationTestClass.class, Setup.class);
        assertEquals("voidMethod", method.getName());
    }

    @Test
    public void testGetAtMostOneVoidMethodWithoutArgs_AnnotationFilter() {
        Method method = getAtMostOneVoidMethodWithoutArgs(AnnotationTestClass.class, Setup.class, ALWAYS_FILTER);
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

    @Test(expected = RuntimeException.class)
    public void testGetAtMostOneVoidMethodWithoutArgs_multipleMethodsFound() {
        getAtMostOneVoidMethodWithoutArgs(AnnotationTestClass.class, Run.class);
    }

    @Test(expected = RuntimeException.class)
    public void testGetAtMostOneVoidMethodWithoutArgs_staticMethodsFound() {
        getAtMostOneVoidMethodWithoutArgs(AnnotationTestClass.class, RunWithWorker.class);
    }

    @Test(expected = RuntimeException.class)
    public void testGetAtMostOneVoidMethodWithoutArgs_wrongReturnTypeArgsFound() {
        getAtMostOneMethodWithoutArgs(AnnotationTestClass.class, Receive.class, String.class);
    }

    @Test(expected = RuntimeException.class)
    public void testGetAtMostOneVoidMethodWithoutArgs_methodsWithArgsFound() {
        getAtMostOneVoidMethodWithoutArgs(AnnotationTestClass.class, Performance.class);
    }

    @SuppressWarnings("unused")
    private static class AnnotationTestClass {

        @Name(value = "testName")
        private Object annotatedField;

        private Object notAnnotatedField;

        @Setup
        private void voidMethod() {
        }

        @Teardown
        private String stringMethod() {
            return null;
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

        @Receive
        private long wrongReturnType() {
            return 0;
        }

        @Performance
        private void hasArguments(String ignored) {
        }
    }

    @SuppressWarnings("all")
    private class TestAnnotation implements Name {

        private final String value;
        private final Class<? extends Annotation> annotationType;

        public TestAnnotation(String value) {
            this(value, Name.class);
        }

        public TestAnnotation(String value, Class<? extends Annotation> annotationType) {
            this.value = value;
            this.annotationType = annotationType;
        }

        @Override
        public String value() {
            return value;
        }

        @Override
        public Class<? extends Annotation> annotationType() {
            return annotationType;
        }
    }
}
