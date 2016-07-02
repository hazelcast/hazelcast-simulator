package com.hazelcast.simulator.utils;

import com.hazelcast.simulator.test.annotations.Run;
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.test.annotations.Warmup;
import org.junit.Test;

import java.lang.reflect.Method;

import static com.hazelcast.simulator.utils.AnnotationReflectionUtils.ALWAYS_FILTER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class AnnotatedMethodRetrieverTest {

    @Test
    public void testGetAtMostOneVoidMethodSkipArgsCheck() {
        Method method = new AnnotatedMethodRetriever(AnnotationTestClass.class, Warmup.class)
                .withVoidReturnType()
                .find();
        assertEquals("voidMethod", method.getName());
    }

    @Test
    public void testGetAtMostOneVoidMethodWithoutArgs() {
        Method method = new AnnotatedMethodRetriever(AnnotationTestClass.class, Warmup.class)
                .withVoidReturnType()
                .withoutArgs()
                .find();

        assertEquals("voidMethod", method.getName());
    }

    @Test
    public void testGetAtMostOneVoidMethodWithoutArgs_AnnotationFilter() {
        Method method = new AnnotatedMethodRetriever(AnnotationTestClass.class, Warmup.class)
                .withFilter(ALWAYS_FILTER)
                .withVoidReturnType()
                .withoutArgs()
                .find();

        assertEquals("voidMethod", method.getName());
    }

    @Test
    public void testGetAtMostOneMethodWithoutArgs() {
        Method method = new AnnotatedMethodRetriever(AnnotationTestClass.class, Teardown.class)
                .withoutArgs()
                .withReturnType(String.class)
                .find();

        assertEquals("stringMethod", method.getName());
    }

    @Test
    public void testGetAtMostOneMethodWithoutArgs_nothingFound() {
        Method method = new AnnotatedMethodRetriever(AnnotationTestClass.class, Verify.class)
                .withoutArgs()
                .withReturnType(Long.class)
                .find();

        assertNull(method);
    }

    @Test(expected = ReflectionException.class)
    public void testGetAtMostOneVoidMethodWithoutArgs_multipleMethodsFound() {
        new AnnotatedMethodRetriever(AnnotationTestClass.class, Run.class)
                .withVoidReturnType()
                .withoutArgs()
                .find();
    }

    @Test(expected = ReflectionException.class)
    public void testGetAtMostOneVoidMethodWithoutArgs_staticMethodsFound() {
        new AnnotatedMethodRetriever(AnnotationTestClass.class, RunWithWorker.class)
                .withVoidReturnType()
                .withoutArgs()
                .withPublicNonStaticModifier()
                .find();
    }

    @Test(expected = ReflectionException.class)
    public void testGetAtMostOneVoidMethodWithoutArgs_wrongReturnTypeArgsFound() {
        new AnnotatedMethodRetriever(AnnotationTestClass.class, Warmup.class)
                .withReturnType(String.class)
                .withoutArgs()
                .find();
    }

    @Test(expected = ReflectionException.class)
    public void testGetAtMostOneVoidMethodWithoutArgs_methodsWithArgsFound() {
        new AnnotatedMethodRetriever(AnnotationTestClass.class, Setup.class)
                .withVoidReturnType()
                .withoutArgs()
                .find();
    }

    @SuppressWarnings("unused")
    public static class AnnotationTestClass {

        @Setup
        public void hasArguments(String ignored) {
        }

        @Teardown
        public String stringMethod() {
            return null;
        }

        @Warmup
        public void voidMethod() {
        }

        @Run
        public void multipleMethod1() {
        }

        @Run
        public void multipleMethod2() {
        }

        @RunWithWorker
        public static void staticMethod() {
        }
    }
}
