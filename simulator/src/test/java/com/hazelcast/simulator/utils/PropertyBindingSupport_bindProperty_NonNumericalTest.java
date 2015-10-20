package com.hazelcast.simulator.utils;

import org.junit.Test;

import java.util.ArrayList;

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
        bindProperty(bindPropertyTestClass, "enumField", BindPropertyTestClass.BindPropertyEnum.HOURS.name());
        assertEquals(bindPropertyTestClass.enumField, BindPropertyTestClass.BindPropertyEnum.HOURS);
    }

    @Test
    public void bindProperty_enum_caseInsensitive() {
        bindProperty(bindPropertyTestClass, "enumField", "dAyS");
        assertEquals(bindPropertyTestClass.enumField, BindPropertyTestClass.BindPropertyEnum.DAYS);
    }

    @Test
    public void bindProperty_enum_privateEnumClass() {
        bindProperty(bindPropertyTestClass, "privateEnumField", BindPropertyTestClass.PrivateBindPropertyEnum.PRIVATE.name());
        assertEquals(bindPropertyTestClass.privateEnumField, BindPropertyTestClass.PrivateBindPropertyEnum.PRIVATE);
    }

    @Test(expected = BindException.class)
    public void bindProperty_enum_fieldNotFound() {
        bindProperty(bindPropertyTestClass, "enumField", "notExist");
    }

    @SuppressWarnings("unused")
    private static class BindPropertyTestClass {

        public enum BindPropertyEnum {
            DAYS,
            HOURS
        }

        private enum PrivateBindPropertyEnum {
            PRIVATE
        }

        public boolean booleanField;
        public Boolean booleanObjectField;

        public Object objectField;
        public String stringField;
        public BindPropertyEnum enumField;
        public PrivateBindPropertyEnum privateEnumField;

        public Class clazz;
    }
}
