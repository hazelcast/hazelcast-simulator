package com.hazelcast.simulator.utils;

import org.junit.Test;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;

import static com.hazelcast.simulator.utils.ReflectionUtils.getField;
import static com.hazelcast.simulator.utils.ReflectionUtils.getFieldValue;
import static com.hazelcast.simulator.utils.ReflectionUtils.getFieldValue0;
import static com.hazelcast.simulator.utils.ReflectionUtils.getFields;
import static com.hazelcast.simulator.utils.ReflectionUtils.getMethodByName;
import static com.hazelcast.simulator.utils.ReflectionUtils.invokeMethod;
import static com.hazelcast.simulator.utils.ReflectionUtils.invokePrivateConstructor;
import static com.hazelcast.simulator.utils.ReflectionUtils.setFieldValue;
import static com.hazelcast.simulator.utils.ReflectionUtils.setFieldValue0;
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
    public void testGetFields() {
        List<Field> fields = getFields(GetFieldByAnnotationTest.class, InjectTest.class);
        assertEquals(3, fields.size());

        Field firstField = fields.get(0);
        assertEquals("firstField", firstField.getName());

        Field secondField = fields.get(1);
        assertEquals("secondField", secondField.getName());

        Field parentField = fields.get(2);
        assertEquals("parentField", parentField.getName());
    }

    @Test
    public void testGetFields_notFound() {
        List<Field> fields = getFields(GetFieldTest.class, InjectTest.class);
        assertTrue(fields.isEmpty());
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
    public void testSetFieldValue() {
        SetFieldTest setFieldTest = new SetFieldTest();
        assertNull(setFieldTest.injectField);

        Field field = getField(SetFieldTest.class, "injectField", Object.class);
        assertNotNull(field);

        setFieldValue(setFieldTest, field, 154915782);
        assertEquals(154915782, setFieldTest.injectField);
    }

    @Test
    public void testGetFieldValue() {
        GetFieldTest getFieldTest = new GetFieldTest();

        Boolean bool = getFieldValue(getFieldTest, "booleanField");
        assertNotNull(bool);
        assertFalse(bool);

        getFieldTest.booleanField = true;
        bool = getFieldValue(getFieldTest, "booleanField");
        assertNotNull(bool);
        assertTrue(bool);
    }

    @Test(expected = NullPointerException.class)
    public void testGetFieldValue_nullObject() {
        getFieldValue(null, "notEvaluated");
    }

    @Test(expected = ReflectionException.class)
    public void testGetFieldValue_notFound() {
        GetFieldTest getFieldTest = new GetFieldTest();

        getFieldValue(getFieldTest, "intField");
    }

    @Test
    public void testGetStaticFieldValue() throws Exception {
        int result = ReflectionUtils.<Integer>getStaticFieldValue(StaticClass.class, "staticField", int.class);
        assertEquals(StaticClass.staticField, result);
    }

    @Test(expected = ReflectionException.class)
    public void testGetStaticFieldValue_nullField() throws Exception {
        int result = ReflectionUtils.<Integer>getStaticFieldValue(StaticClass.class, "notFound", int.class);
        assertEquals(StaticClass.staticField, result);
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

    @Test(expected = AssertionError.class)
    public void testInvokeMethodThrowsError() throws Exception {
        Method method = getMethodByName(InvokeMethodTest.class, "throwErrorMethod");
        invokeMethod(new InvokeMethodTest(), method);
    }

    @Test(expected = IllegalAccessException.class)
    public void testInvokeMethodPrivate() throws Exception {
        Method method = getMethodByName(InvokeMethodTest.class, "cannotAccessMethod");
        invokeMethod(new InvokeMethodTest(), method);
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

    @Test
    public void testInvokePrivateConstructor() throws Exception {
        assertFalse(PrivateConstructorTest.hasBeenConstructed);

        invokePrivateConstructor(PrivateConstructorTest.class);

        assertTrue(PrivateConstructorTest.hasBeenConstructed);
    }

    @Test(expected = ReflectionException.class)
    public void testSetFieldValueInternal_privateField() {
        SetFieldTest setFieldTest = new SetFieldTest();
        Field field = getField(SetFieldTest.class, "injectField", Object.class);
        setFieldValue0(setFieldTest, field, "value");
    }

    @Test(expected = ReflectionException.class)
    public void testGetFieldValueInternal_privateField() {
        Field field = getField(StaticClass.class, "staticField", int.class);
        getFieldValue0(null, field, "StaticClass", "staticField");
    }

    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    private @interface InjectTest {
    }

    @SuppressWarnings("unused")
    private static final class GetFieldByAnnotationTest extends GetFieldByAnnotationParent {

        @InjectTest
        private String firstField;

        @InjectTest
        private String secondField;
    }

    @SuppressWarnings("unused")
    private static class GetFieldByAnnotationParent {

        @InjectTest
        private String parentField;
    }

    @SuppressWarnings("unused")
    private static final class GetFieldTest extends GetFieldParent {

        private boolean booleanField;
    }

    @SuppressWarnings("unused")
    private static class GetFieldParent {

        private static int intField;
    }

    private static final class StaticClass {

        private static int staticField = 10;
    }

    @SuppressWarnings("unused")
    private static final class SetFieldTest {

        private Object injectField;
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

        public void throwErrorMethod() {
            throw new AssertionError("expected exception");
        }

        private void cannotAccessMethod() {
        }
    }

    private static final class PrivateConstructorTest {

        private static boolean hasBeenConstructed;

        private PrivateConstructorTest() {
            hasBeenConstructed = true;
        }
    }
}
