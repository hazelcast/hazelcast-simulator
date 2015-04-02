package com.hazelcast.simulator.utils;

import org.junit.Test;

import static com.hazelcast.simulator.utils.PropertyBindingSupport.bindProperty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class PropertyBindingSupport_bindProperty_NumericalTest {

    private final BindPropertyTestClass bindPropertyTestClass = new BindPropertyTestClass();

    @Test
    public void bindProperty_byte() throws IllegalAccessException {
        bindProperty(bindPropertyTestClass, "byteField", "10");
        assertEquals(10, bindPropertyTestClass.byteField);
    }

    @Test
    public void bindProperty_Byte() throws IllegalAccessException {
        bindProperty(bindPropertyTestClass, "byteObjectField", "null");
        assertNull(bindPropertyTestClass.byteObjectField);

        bindProperty(bindPropertyTestClass, "byteObjectField", "10");
        assertEquals(new Byte("10"), bindPropertyTestClass.byteObjectField);
    }

    @Test(expected = BindException.class)
    public void bindProperty_Byte_invalid() throws IllegalAccessException {
        bindProperty(bindPropertyTestClass, "byteObjectField", "invalid");
    }

    @Test
    public void bindProperty_short() throws IllegalAccessException {
        bindProperty(bindPropertyTestClass, "shortField", "10");
        assertEquals(10, bindPropertyTestClass.shortField);
    }

    @Test
    public void bindProperty_Short() throws IllegalAccessException {
        bindProperty(bindPropertyTestClass, "shortObjectField", "null");
        assertNull(bindPropertyTestClass.shortObjectField);

        bindProperty(bindPropertyTestClass, "shortObjectField", "10");
        assertEquals(new Short("10"), bindPropertyTestClass.shortObjectField);
    }

    @Test(expected = BindException.class)
    public void bindProperty_Short_invalid() throws IllegalAccessException {
        bindProperty(bindPropertyTestClass, "shortObjectField", "invalid");
    }

    @Test
    public void bindProperty_int() throws IllegalAccessException {
        bindProperty(bindPropertyTestClass, "intField", "10");
        assertEquals(10, bindPropertyTestClass.intField);
    }

    @Test
    public void bindProperty_Integer() throws IllegalAccessException {
        bindProperty(bindPropertyTestClass, "integerField", "null");
        assertNull(bindPropertyTestClass.integerField);

        bindProperty(bindPropertyTestClass, "integerField", "10");
        assertEquals(new Integer(10), bindPropertyTestClass.integerField);
    }

    @Test(expected = BindException.class)
    public void bindProperty_Integer_invalid() throws IllegalAccessException {
        bindProperty(bindPropertyTestClass, "integerObjectField", "invalid");
    }

    @Test
    public void bindProperty_long() throws IllegalAccessException {
        bindProperty(bindPropertyTestClass, "longField", "1234567890123");
        assertEquals(1234567890123L, bindPropertyTestClass.longField);
    }

    @Test
    public void bindProperty_Long() throws IllegalAccessException {
        bindProperty(bindPropertyTestClass, "longObjectField", "null");
        assertNull(bindPropertyTestClass.longObjectField);

        bindProperty(bindPropertyTestClass, "longObjectField", "1234567890123");
        assertEquals(new Long("1234567890123"), bindPropertyTestClass.longObjectField);
    }

    @Test(expected = BindException.class)
    public void bindProperty_Long_invalid() throws IllegalAccessException {
        bindProperty(bindPropertyTestClass, "longObjectField", "invalid");
    }

    @Test
    public void bindProperty_float() throws IllegalAccessException {
        bindProperty(bindPropertyTestClass, "floatField", "23.42");
        assertEquals(23.42f, bindPropertyTestClass.floatField, 0.01);
    }

    @Test
    public void bindProperty_Float() throws IllegalAccessException {
        bindProperty(bindPropertyTestClass, "floatObjectField", "null");
        assertNull(bindPropertyTestClass.floatObjectField);

        bindProperty(bindPropertyTestClass, "floatObjectField", "23.42");
        assertEquals(new Float(23.42f), bindPropertyTestClass.floatObjectField);
    }

    @Test(expected = BindException.class)
    public void bindProperty_Float_invalid() throws IllegalAccessException {
        bindProperty(bindPropertyTestClass, "floatObjectField", "invalid");
    }

    @Test
    public void bindProperty_double() throws IllegalAccessException {
        bindProperty(bindPropertyTestClass, "doubleField", "23420000000000.2342");
        assertEquals(23420000000000.2342d, bindPropertyTestClass.doubleField, 0.0001);
    }

    @Test
    public void bindProperty_Double() throws IllegalAccessException {
        bindProperty(bindPropertyTestClass, "doubleObjectField", "null");
        assertNull(bindPropertyTestClass.doubleObjectField);

        bindProperty(bindPropertyTestClass, "doubleObjectField", "23420000000000.2342");
        assertEquals(new Double(23420000000000.2342d), bindPropertyTestClass.doubleObjectField);
    }

    @Test(expected = BindException.class)
    public void bindProperty_Double_invalid() throws IllegalAccessException {
        bindProperty(bindPropertyTestClass, "doubleObjectField", "invalid");
    }

    @SuppressWarnings("unused")
    private class BindPropertyTestClass {

        private byte byteField;
        private Byte byteObjectField;

        private short shortField;
        private Short shortObjectField;

        private int intField;
        private Integer integerField;

        private long longField;
        private Long longObjectField;

        private float floatField;
        private Float floatObjectField;

        private double doubleField;
        private Double doubleObjectField;
    }
}
