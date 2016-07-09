package com.hazelcast.simulator.utils;

import org.junit.Test;

import java.util.ArrayList;

import static com.hazelcast.simulator.utils.PropertyBindingSupport.bind0;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class PropertyBindingSupport_bind0_NonNumericalTest {

    private final BindPropertyTestClass bindPropertyTestClass = new BindPropertyTestClass();

    @Test
    public void bindProperty_boolean() {
        bind0(bindPropertyTestClass, "booleanField", "true");
        assertEquals(true, bindPropertyTestClass.booleanField);

        bind0(bindPropertyTestClass, "booleanField", "false");
        assertEquals(false, bindPropertyTestClass.booleanField);
    }

    @Test(expected = BindException.class)
    public void bindProperty_boolean_invalid() {
        bind0(bindPropertyTestClass, "booleanField", "invalid");
    }

    @Test
    public void bindProperty_Boolean() {
        bind0(bindPropertyTestClass, "booleanObjectField", "null");
        assertNull(bindPropertyTestClass.booleanObjectField);

        bind0(bindPropertyTestClass, "booleanObjectField", "true");
        assertEquals(Boolean.TRUE, bindPropertyTestClass.booleanObjectField);

        bind0(bindPropertyTestClass, "booleanObjectField", "false");
        assertEquals(Boolean.FALSE, bindPropertyTestClass.booleanObjectField);
    }

    @Test(expected = BindException.class)
    public void bindProperty_Boolean_invalid() {
        bind0(bindPropertyTestClass, "booleanObjectField", "invalid");
    }

    @Test
    public void bindProperty_class() {
        bind0(bindPropertyTestClass, "clazz", "null");
        assertNull(bindPropertyTestClass.clazz);

        bind0(bindPropertyTestClass, "clazz", ArrayList.class.getName());
        assertEquals(ArrayList.class.getName(), bindPropertyTestClass.clazz.getName());
    }

    @Test(expected = BindException.class)
    public void bindProperty_class_notFound() {
        bind0(bindPropertyTestClass, "clazz", "com.hazelnuts.simulator.utils.NotFound");
    }

    @Test
    public void bindProperty_string() {
        bind0(bindPropertyTestClass, "stringField", "null");
        assertNull(bindPropertyTestClass.stringField);

        bind0(bindPropertyTestClass, "stringField", "foo");
        assertEquals("foo", bindPropertyTestClass.stringField);
    }

    @Test
    public void bindProperty_enum_nullValue() {
        bind0(bindPropertyTestClass, "enumField", "null");
        assertNull(bindPropertyTestClass.enumField);
    }

    @Test
    public void bindProperty_enum() {
        bind0(bindPropertyTestClass, "enumField", BindPropertyTestClass.BindPropertyEnum.HOURS.name());
        assertEquals(bindPropertyTestClass.enumField, BindPropertyTestClass.BindPropertyEnum.HOURS);
    }

    @Test
    public void bindProperty_enum_caseInsensitive() {
        bind0(bindPropertyTestClass, "enumField", "dAyS");
        assertEquals(bindPropertyTestClass.enumField, BindPropertyTestClass.BindPropertyEnum.DAYS);
    }

    @Test
    public void bindProperty_enum_privateEnumClass() {
        bind0(bindPropertyTestClass, "privateEnumField", BindPropertyTestClass.PrivateBindPropertyEnum.PRIVATE.name());
        assertEquals(bindPropertyTestClass.privateEnumField, BindPropertyTestClass.PrivateBindPropertyEnum.PRIVATE);
    }

    @Test(expected = BindException.class)
    public void bindProperty_enum_fieldNotFound() {
        bind0(bindPropertyTestClass, "enumField", "notExist");
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
