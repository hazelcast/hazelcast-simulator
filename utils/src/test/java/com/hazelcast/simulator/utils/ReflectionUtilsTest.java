package com.hazelcast.simulator.utils;

import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

import static com.hazelcast.simulator.utils.ReflectionUtils.getField;
import static com.hazelcast.simulator.utils.ReflectionUtils.getMethodByName;
import static com.hazelcast.simulator.utils.ReflectionUtils.getObjectFromField;
import static com.hazelcast.simulator.utils.ReflectionUtils.getStaticFieldValue;
import static com.hazelcast.simulator.utils.ReflectionUtils.injectObjectToInstance;
import static com.hazelcast.simulator.utils.ReflectionUtils.invokeMethod;
import static com.hazelcast.simulator.utils.ReflectionUtils.invokePrivateConstructor;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ReflectionUtilsTest {

    @Test
    public void testGetStaticFieldValue() throws Exception {
        int result = (Integer) getStaticFieldValue(StaticClass.class, "staticField", int.class);
        assertEquals(StaticClass.staticField, result);
    }

    static class StaticClass {
        static int staticField = 10;
    }

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
    public void testInvokeMethodNull() throws Exception {
        assertNull(invokeMethod(new InvokeMethodTest(), null));
    }

    @Test
    public void testInvokeMethod() throws Exception {
        assertFalse(InvokeMethodTest.hasBeenInvoked);

        Method method = getMethodByName(InvokeMethodTest.class, "testMethod");
        invokeMethod(new InvokeMethodTest(), method);

        assertTrue(InvokeMethodTest.hasBeenInvoked);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvokeMethodThrowsException() throws Exception {
        Method method = getMethodByName(InvokeMethodTest.class, "throwExceptionMethod");
        invokeMethod(new InvokeMethodTest(), method);
    }

    @Test(expected = IllegalAccessException.class)
    public void testInvokeMethodPrivate() throws Exception {
        Method method = getMethodByName(InvokeMethodTest.class, "cannotAccessMethod");
        invokeMethod(new InvokeMethodTest(), method);
    }

    @Test
    public void testGetField() {
        Field field = getField(GetFieldTest.class, "booleanField", Boolean.TYPE);
        assertNotNull(field);
        assertEquals(field.getType().getName(), Boolean.TYPE.getName());
    }

    @Test
    public void testGetField_fromSuperclass() {
        Field field = getField(GetFieldTest.class, "intField", Integer.TYPE);
        assertNotNull(field);
        assertEquals(field.getType().getName(), Integer.TYPE.getName());
    }

    @Test
    public void testGetField_notFound() {
        Field field = getField(GetFieldTest.class, "notFound", null);
        assertNull(field);
    }

    @Test
    public void testGetField_primitive() {
        Field field = getField(GetFieldTest.class, "booleanField", null);
        assertNotNull(field);
        assertEquals(field.getType().getName(), Boolean.TYPE.getName());
    }

    @Test
    public void testGetField_typeMismatch() {
        Field field = getField(GetFieldTest.class, "booleanField", Integer.TYPE);
        assertNull(field);
    }

    @Test
    public void testGetObjectFromField() {
        GetFieldTest getFieldTest = new GetFieldTest();

        Boolean bool = getObjectFromField(getFieldTest, "booleanField");
        assertNotNull(bool);
        assertFalse(bool);

        getFieldTest.booleanField = true;
        bool = getObjectFromField(getFieldTest, "booleanField");
        assertNotNull(bool);
        assertTrue(bool);
    }

    @Test(expected = NullPointerException.class)
    public void testGetObjectFromField_nullObject() {
        getObjectFromField(null, "notEvaluated");
    }

    @Test(expected = ReflectionException.class)
    public void testGetObjectFromField_notFound() {
        GetFieldTest getFieldTest = new GetFieldTest();

        getObjectFromField(getFieldTest, "intField");
    }

    @Test
    public void testInjectObjectToInstance() {
        InjectTest injectTest = new InjectTest();
        assertNull(injectTest.injectField);

        Field field = getField(InjectTest.class, "injectField", Object.class);
        assertNotNull(field);

        injectObjectToInstance(injectTest, field, 154915782);
        assertEquals(154915782, injectTest.injectField);
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

    @SuppressWarnings("unused")
    private static final class GetFieldTest extends GetFieldParent {

        private boolean booleanField;
    }

    @SuppressWarnings("unused")
    private static class GetFieldParent {

        private static int intField;
    }

    @SuppressWarnings("unused")
    private static final class InjectTest {

        private Object injectField;
    }
}
