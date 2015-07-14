package com.hazelcast.simulator.utils;

import org.junit.Test;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.simulator.utils.PropertyBindingSupport.bindProperty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class PropertyBindingSupport_bindProperty_NonNumericalTest {

    private final BindPropertyTestClass bindPropertyTestClass = new BindPropertyTestClass();

    @Test
    public void bindProperty_boolean() {
        bindProperty(bindPropertyTestClass, "booleanField", "true");
        assertEquals(true, bindPropertyTestClass.booleanField);

        bindProperty(bindPropertyTestClass, "booleanField", "false");
        assertEquals(false, bindPropertyTestClass.booleanField);
    }

    @Test(expected = BindException.class)
    public void bindProperty_boolean_invalid() {
        bindProperty(bindPropertyTestClass, "booleanField", "invalid");
    }

    @Test
    public void bindProperty_Boolean() {
        bindProperty(bindPropertyTestClass, "booleanObjectField", "null");
        assertNull(bindPropertyTestClass.booleanObjectField);

        bindProperty(bindPropertyTestClass, "booleanObjectField", "true");
        assertEquals(Boolean.TRUE, bindPropertyTestClass.booleanObjectField);

        bindProperty(bindPropertyTestClass, "booleanObjectField", "false");
        assertEquals(Boolean.FALSE, bindPropertyTestClass.booleanObjectField);
    }

    @Test(expected = BindException.class)
    public void bindProperty_Boolean_invalid() {
        bindProperty(bindPropertyTestClass, "booleanObjectField", "invalid");
    }

    @Test
    public void bindProperty_class() {
        bindProperty(bindPropertyTestClass, "clazz", "null");
        assertNull(bindPropertyTestClass.clazz);

        bindProperty(bindPropertyTestClass, "clazz", ArrayList.class.getName());
        assertEquals(ArrayList.class.getName(), bindPropertyTestClass.clazz.getName());
    }

    @Test(expected = BindException.class)
    public void bindProperty_class_notFound() {
        bindProperty(bindPropertyTestClass, "clazz", "com.hazelnuts.simulator.utils.NotFound");
    }

    @Test
    public void bindProperty_string() {
        bindProperty(bindPropertyTestClass, "stringField", "null");
        assertNull(bindPropertyTestClass.stringField);

        bindProperty(bindPropertyTestClass, "stringField", "foo");
        assertEquals("foo", bindPropertyTestClass.stringField);
    }

    @Test
    public void bindProperty_enum_nullValue() {
        bindProperty(bindPropertyTestClass, "enumField", "null");
        assertNull(bindPropertyTestClass.enumField);
    }

    @Test
    public void bindProperty_enum() {
        bindProperty(bindPropertyTestClass, "enumField", TimeUnit.HOURS.name());
        assertEquals(bindPropertyTestClass.enumField, TimeUnit.HOURS);
    }

    @Test
    public void bindProperty_enum_caseInsensitive() {
        bindProperty(bindPropertyTestClass, "enumField", "dAyS");
        assertEquals(bindPropertyTestClass.enumField, TimeUnit.DAYS);
    }

    @Test(expected = BindException.class)
    public void bindProperty_enum_notFound() {
        bindProperty(bindPropertyTestClass, "enumField", "notExist");
    }

    @Test(expected = BindException.class)
    public void bindProperty_unknownField() {
        bindProperty(bindPropertyTestClass, "notExist", "null");
    }

    @Test
    public void bindProperty_withPath() {
        bindProperty(bindPropertyTestClass, "otherObject.stringField", "newValue");
        assertEquals("newValue", bindPropertyTestClass.otherObject.stringField);
    }

    @Test(expected = BindException.class)
    public void bindProperty_withPathAndNullValue() {
        bindProperty(bindPropertyTestClass, "nullOtherObject.stringField", "newValue");
    }

    @Test(expected = BindException.class)
    public void bindProperty_withPath_missingProperty() {
        bindProperty(bindPropertyTestClass, "notExist.stringField", "newValue");
    }

    @Test(expected = BindException.class)
    public void bindProperty_staticField() {
        bindProperty(bindPropertyTestClass.otherObject, "staticField", "newValue");
    }

    @Test(expected = BindException.class)
    public void bindProperty_finalField() {
        bindProperty(bindPropertyTestClass.otherObject, "finalField", "newValue");
    }

    @Test(expected = BindException.class)
    public void bindProperty_privateField() {
        bindProperty(bindPropertyTestClass.otherObject, "privateField", "newValue");
    }

    @Test(expected = BindException.class)
    public void bindProperty_protectedField() {
        bindProperty(bindPropertyTestClass.otherObject, "protectedField", "newValue");
    }

    @Test(expected = BindException.class)
    public void bindProperty_packageFriendlyField() {
        bindProperty(bindPropertyTestClass.otherObject, "packageFriendlyField", "newValue");
    }

    @Test(expected = BindException.class)
    public void bindProperty_fallsThroughAllChecks() {
        bindProperty(bindPropertyTestClass, "comparableField", "newValue");
    }

    @SuppressWarnings("unused")
    private class BindPropertyTestClass {

        public boolean booleanField;
        public Boolean booleanObjectField;

        public Object objectField;
        public String stringField;
        public TimeUnit enumField;
        public Comparable comparableField;

        public OtherObject otherObject = new OtherObject();
        public OtherObject nullOtherObject;
        public Class clazz;

        private String privateField;
        protected String protectedField;
        String packageFriendlyField;
    }

    @SuppressWarnings("unused")
    private static class OtherObject {

        private static Object staticField;

        public final int finalField = 5;

        public String stringField;
    }
}
