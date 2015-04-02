package com.hazelcast.simulator.utils;

import com.hazelcast.simulator.test.TestCase;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.simulator.utils.PropertyBindingSupport.bindOptionalProperty;
import static com.hazelcast.simulator.utils.PropertyBindingSupport.bindProperties;
import static com.hazelcast.simulator.utils.PropertyBindingSupport.bindProperty;
import static com.hazelcast.simulator.utils.ReflectionUtils.invokePrivateConstructor;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class PropertyBindingSupportTest {

    private final BindPropertyTestClass bindPropertyTestClass = new BindPropertyTestClass();

    @Test
    public void testConstructor() throws Exception {
        invokePrivateConstructor(PropertyBindingSupport.class);
    }

    @Test
    public void bindProperties_notFound_optional() throws NoSuchFieldException, IllegalAccessException {
        TestCase testCase = new TestCase();
        testCase.setProperty("class", "willBeIgnored");
        testCase.setProperty("probe-getLatency", "willBeIgnored");
        testCase.setProperty("notExist", "isOptional");

        Set<String> optionalProperties = new HashSet<String>();
        optionalProperties.add("notExist");

        bindProperties(bindPropertyTestClass, testCase, optionalProperties);
    }

    @Test
    public void testBindOptionalProperty_testcaseIsNull() throws NoSuchFieldException, IllegalAccessException {
        bindOptionalProperty(bindPropertyTestClass, null, "ignored");
    }

    @Test
    public void testBindOptionalProperty_propertyNotDefined() throws NoSuchFieldException, IllegalAccessException {
        TestCase testCase = new TestCase();
        testCase.setProperty("class", "willBeIgnored");
        testCase.setProperty("probe-getLatency", "willBeIgnored");
        testCase.setProperty("notExist", "isOptional");

        bindOptionalProperty(bindPropertyTestClass, testCase, "propertyNotDefined");
    }

    @Test
    public void testBindOptionalProperty_propertyNotFound() throws NoSuchFieldException, IllegalAccessException {
        TestCase testCase = new TestCase();
        testCase.setProperty("class", "willBeIgnored");
        testCase.setProperty("probe-getLatency", "willBeIgnored");
        testCase.setProperty("notExist", "isOptional");

        bindOptionalProperty(bindPropertyTestClass, testCase, "notExist");
    }

    @Test
    public void testBindOptionalProperty() throws NoSuchFieldException, IllegalAccessException {
        TestCase testCase = new TestCase();
        testCase.setProperty("class", "willBeIgnored");
        testCase.setProperty("probe-getLatency", "willBeIgnored");
        testCase.setProperty("stringField", "foo");

        bindOptionalProperty(bindPropertyTestClass, testCase, "stringField");
        assertEquals(bindPropertyTestClass.stringField, "foo");
    }

    @Test
    public void bindProperty_string() throws IllegalAccessException {
        bindProperty(bindPropertyTestClass, "stringField", "null");
        assertNull(bindPropertyTestClass.stringField);

        bindProperty(bindPropertyTestClass, "stringField", "foo");
        assertEquals(bindPropertyTestClass.stringField, "foo");
    }

    @Test
    public void bindProperty_class() throws IllegalAccessException {
        bindProperty(bindPropertyTestClass, "clazz", "null");
        assertNull(bindPropertyTestClass.clazz);

        bindProperty(bindPropertyTestClass, "clazz", ArrayList.class.getName());
        assertEquals(bindPropertyTestClass.clazz, ArrayList.class);
    }

    @Test
    public void bindProperty_enum_nullValue() throws IllegalAccessException {
        bindProperty(bindPropertyTestClass, "enumField", "null");
        assertNull(bindPropertyTestClass.enumField);
    }

    @Test
    public void bindProperty_enum() throws IllegalAccessException {
        bindProperty(bindPropertyTestClass, "enumField", TimeUnit.HOURS.name());
        assertEquals(bindPropertyTestClass.enumField, TimeUnit.HOURS);
    }

    @Test
    public void bindProperty_enum_caseInsensitive() throws IllegalAccessException {
        bindProperty(bindPropertyTestClass, "enumField", "dAyS");
        assertEquals(bindPropertyTestClass.enumField, TimeUnit.DAYS);
    }

    @Test(expected = BindException.class)
    public void bindProperty_enum_notFound() throws IllegalAccessException {
        bindProperty(bindPropertyTestClass, "enumField", "notExist");
    }

    @Test
    public void bindProperty_boolean() throws IllegalAccessException {
        bindProperty(bindPropertyTestClass, "booleanField", "true");
        assertEquals(bindPropertyTestClass.booleanField, true);

        bindProperty(bindPropertyTestClass, "booleanField", "false");
        assertEquals(bindPropertyTestClass.booleanField, false);
    }

    @Test(expected = BindException.class)
    public void bindProperty_boolean_invalid() throws IllegalAccessException {
        bindProperty(bindPropertyTestClass, "booleanField", "invalid");
    }

    @Test
    public void bindProperty_Boolean() throws IllegalAccessException {
        bindProperty(bindPropertyTestClass, "booleanObjectField", "null");
        assertNull(bindPropertyTestClass.booleanObjectField);

        bindProperty(bindPropertyTestClass, "booleanObjectField", "true");
        assertEquals(bindPropertyTestClass.booleanObjectField, true);

        bindProperty(bindPropertyTestClass, "booleanObjectField", "false");
        assertEquals(bindPropertyTestClass.booleanObjectField, false);
    }

    @Test(expected = BindException.class)
    public void bindProperty_Boolean_invalid() throws IllegalAccessException {
        bindProperty(bindPropertyTestClass, "booleanObjectField", "invalid");
    }

    @Test
    public void bindProperty_int() throws IllegalAccessException {
        bindProperty(bindPropertyTestClass, "intField", "10");
        assertEquals(bindPropertyTestClass.intField, 10);
    }

    @Test
    public void bindProperty_Integer() throws IllegalAccessException {
        bindProperty(bindPropertyTestClass, "integerField", "null");
        assertNull(bindPropertyTestClass.integerField);

        bindProperty(bindPropertyTestClass, "integerField", "10");
        assertEquals(bindPropertyTestClass.integerField, new Integer(10));
    }

    @Test(expected = BindException.class)
    public void bindProperty_unknownField() throws IllegalAccessException {
        bindProperty(bindPropertyTestClass, "notExist", "null");
    }

    @Test
    public void bindProperty_withPath() throws IllegalAccessException {
        bindProperty(bindPropertyTestClass, "otherObject.stringField", "newValue");
        assertEquals("newValue", bindPropertyTestClass.otherObject.stringField);
    }

    @Test(expected = BindException.class)
    public void bindProperty_withPathAndNullValue() throws IllegalAccessException {
        bindProperty(bindPropertyTestClass, "nullOtherObject.stringField", "newValue");
    }

    @Test(expected = BindException.class)
    public void bindProperty_withPath_missingProperty() throws IllegalAccessException {
        bindProperty(bindPropertyTestClass, "notExist.stringField", "newValue");
    }

    @SuppressWarnings("unused")
    private class BindPropertyTestClass {

        public OtherObject otherObject = new OtherObject();
        public OtherObject nullOtherObject;
        public Class clazz;

        private Object objectField;
        private String stringField;
        private TimeUnit enumField;

        private boolean booleanField;
        private Boolean booleanObjectField;

        private int intField;
        private Integer integerField;
    }

    private class OtherObject {

        public String stringField;
    }
}
