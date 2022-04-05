package com.hazelcast.simulator.utils;

import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Run;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.Verify;
import org.junit.Test;

import java.lang.reflect.Method;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class AnnotatedMethodRetrieverTest {

    @Test
    public void testGetAtMostOneVoidMethodSkipArgsCheck() {
        Method method = new AnnotatedMethodRetriever(AnnotationTestClass.class, Prepare.class)
                .withVoidReturnType()
                .find();
        assertEquals("voidMethod", method.getName());
    }

    @Test
    public void testGetAtMostOneVoidMethodWithoutArgs() {
        Method method = new AnnotatedMethodRetriever(AnnotationTestClass.class, Prepare.class)
                .withVoidReturnType()
                .withoutArgs()
                .find();

        assertEquals("voidMethod", method.getName());
    }

    @Test
    public void testGetAtMostOneVoidMethodWithoutArgs_AnnotationFilter() {
        Method method = new AnnotatedMethodRetriever(AnnotationTestClass.class, Prepare.class)
                .withVoidReturnType()
                .withoutArgs()
                .find();

        assertEquals("voidMethod", method.getName());
    }

    @Test
    public void testGetAtMostOneMethodWithoutArgs() {
        Method method = new AnnotatedMethodRetriever(TestGetAtMostOneMethodWithoutArgs.class, Teardown.class)
                .withoutArgs()
                .withReturnType(String.class)
                .find();

        assertEquals("stringMethod", method.getName());
    }

    @Test
    public void testGetAtMostOneMethodWithoutArgs_nothingFound() {
        Method method = new AnnotatedMethodRetriever(TestGetAtMostOneMethodWithoutArgs.class, Setup.class)
                .withoutArgs()
                .withReturnType(Long.class)
                .find();

        assertNull(method);
    }

    public class TestGetAtMostOneMethodWithoutArgs {
        @Teardown
        public String stringMethod() {
            return null;
        }
    }

    @Test(expected = ReflectionException.class)
    public void testGetAtMostOneVoidMethodWithoutArgs_multipleMethodsFound() {
        new AnnotatedMethodRetriever(AnnotationTestClass.class, Run.class)
                .withVoidReturnType()
                .withoutArgs()
                .find();
    }

    @Test(expected = ReflectionException.class)
    public void testGetAtMostOneVoidMethodWithoutArgs_wrongReturnTypeArgsFound() {
        new AnnotatedMethodRetriever(AnnotationTestClass.class, Prepare.class)
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

    @Test
    public void testSubClass() {
        List<Method> methodList = new AnnotatedMethodRetriever(Subclass.class, Verify.class)
                .withFilter(new AnnotationFilter.VerifyFilter(true))
                .withVoidReturnType()
                .withoutArgs()
                .findAll();

        assertEquals(2, methodList.size());
    }

    @Test
    public void testSubClass_methodFoundInSuper() {
        List<Method> methodList = new AnnotatedMethodRetriever(Subclass.class, Teardown.class)
                .withFilter(new AnnotationFilter.TeardownFilter(false))
                .withVoidReturnType()
                .withoutArgs()
                .findAll();

        assertEquals(1, methodList.size());
        assertEquals("tearDown", methodList.get(0).getName());
    }

    public static class SuperClass {
        @Verify
        public void verify() {

        }

        @Teardown
        public void tearDown() {

        }

    }

    public static class Subclass extends SuperClass {
        @Verify
        public void verify() {

        }

        @Verify
        public void verify2() {

        }
    }

    @SuppressWarnings("unused")
    public static class AnnotationTestClass {

        @Setup
        public void setup1(String ignored) {
        }

        @Teardown
        public String stringMethod() {
            return null;
        }

        @Prepare
        public void voidMethod() {
        }

        @Run
        public void multipleMethod1() {
        }

        @Run
        public void multipleMethod2() {
        }

        @Verify
        public void verify() {
        }
    }

    public class AnnotatedSubTestClass extends AnnotationTestClass {
        @Verify
        public void verify2() {
        }
    }
}
