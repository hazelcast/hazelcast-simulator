package com.hazelcast.simulator.utils;

import org.junit.Test;

import java.lang.reflect.Method;

import static com.hazelcast.simulator.utils.ReflectionUtils.getMethodByName;
import static com.hazelcast.simulator.utils.ReflectionUtils.invokeMethod;
import static com.hazelcast.simulator.utils.ReflectionUtils.invokePrivateConstructor;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ReflectionUtilsTest {

    @Test
    public void testConstructor() throws Exception {
        invokePrivateConstructor(ReflectionUtils.class);
    }

    @Test
    public void testInvokePrivateConstructor() throws Exception {
        assertFalse(PrivateConstructorTest.hasBeenConstructed);

        invokePrivateConstructor(PrivateConstructorTest.class);

        assertTrue(PrivateConstructorTest.hasBeenConstructed);
    }

    @Test
    public void testGetMethodByName() {
        Method method = getMethodByName(InvokeMethodTest.class, "testMethod");
        assertNotNull(method);
        assertEquals(Void.TYPE, method.getReturnType());
    }

    @Test
    public void testGetMethodByNameNotFound() {
        Method method = getMethodByName(InvokeMethodTest.class, "testMethodNotFound");
        assertNull(method);
    }

    @Test()
    public void testInvokeMethodNull() throws Throwable {
        assertNull(invokeMethod(new InvokeMethodTest(), null));
    }

    @Test
    public void testInvokeMethod() throws Throwable {
        assertFalse(InvokeMethodTest.hasBeenInvoked);

        Method method = getMethodByName(InvokeMethodTest.class, "testMethod");
        invokeMethod(new InvokeMethodTest(), method);

        assertTrue(InvokeMethodTest.hasBeenInvoked);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvokeMethodThrowsException() throws Throwable {
        Method method = getMethodByName(InvokeMethodTest.class, "throwExceptionMethod");
        invokeMethod(new InvokeMethodTest(), method);
    }

    @Test(expected = IllegalAccessException.class)
    public void testInvokeMethodPrivate() throws Throwable {
        Method method = getMethodByName(InvokeMethodTest.class, "cannotAccessMethod");
        invokeMethod(new InvokeMethodTest(), method);
    }

    private static final class PrivateConstructorTest {

        private static boolean hasBeenConstructed;

        private PrivateConstructorTest() {
            hasBeenConstructed = true;
        }
    }

    @SuppressWarnings("unused")
    private static final class InvokeMethodTest {

        private static boolean hasBeenInvoked;

        public void testMethod() {
            hasBeenInvoked = true;
        }

        public void throwExceptionMethod() {
            throw new IllegalArgumentException("expected exception");
        }

        private void cannotAccessMethod() {
        }
    }
}
