package com.hazelcast.simulator.utils;

import org.junit.Test;

import static com.hazelcast.simulator.utils.FormatUtils.NEW_LINE;
import static com.hazelcast.simulator.utils.PropertyBindingSupport.bindProperty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class PropertyBindingSupport_bindProperty_NumericalTest {

    private final BindPropertyTestClass bindPropertyTestClass = new BindPropertyTestClass();

    @Test
    public void bindProperty_byte() {
        bindProperty(bindPropertyTestClass, "byteField", "10");
        assertEquals(10, bindPropertyTestClass.byteField);
    }

    @Test
    public void bindProperty_byte_whitespace() {
        bindProperty(bindPropertyTestClass, "byteField", " 10 ");
        assertEquals(10, bindPropertyTestClass.byteField);
    }

    @Test
    public void bindProperty_Byte() {
        bindProperty(bindPropertyTestClass, "byteObjectField", "null");
        assertNull(bindPropertyTestClass.byteObjectField);

        bindProperty(bindPropertyTestClass, "byteObjectField", "10");
        assertEquals(new Byte("10"), bindPropertyTestClass.byteObjectField);
    }

    @Test(expected = BindException.class)
    public void bindProperty_Byte_invalid() {
        bindProperty(bindPropertyTestClass, "byteObjectField", "invalid");
    }

    @Test
    public void bindProperty_short() {
        bindProperty(bindPropertyTestClass, "shortField", "10");
        assertEquals(10, bindPropertyTestClass.shortField);
    }

    @Test
    public void bindProperty_short_whitespace() {
        bindProperty(bindPropertyTestClass, "shortField", " 10");
        assertEquals(10, bindPropertyTestClass.shortField);
    }

    @Test
    public void bindProperty_Short() {
        bindProperty(bindPropertyTestClass, "shortObjectField", "null");
        assertNull(bindPropertyTestClass.shortObjectField);

        bindProperty(bindPropertyTestClass, "shortObjectField", "10");
        assertEquals(new Short("10"), bindPropertyTestClass.shortObjectField);
    }

    @Test(expected = BindException.class)
    public void bindProperty_Short_invalid() {
        bindProperty(bindPropertyTestClass, "shortObjectField", "invalid");
    }

    @Test
    public void bindProperty_int() {
        bindProperty(bindPropertyTestClass, "intField", "10");
        assertEquals(10, bindPropertyTestClass.intField);
    }

    @Test
    public void bindProperty_int_whitespace() {
        bindProperty(bindPropertyTestClass, "intField", "10" + NEW_LINE);
        assertEquals(10, bindPropertyTestClass.intField);
    }

    @Test
    public void bindProperty_Integer() {
        bindProperty(bindPropertyTestClass, "integerField", "null");
        assertNull(bindPropertyTestClass.integerField);

        bindProperty(bindPropertyTestClass, "integerField", "10");
        assertEquals(new Integer(10), bindPropertyTestClass.integerField);
    }

    @Test(expected = BindException.class)
    public void bindProperty_Integer_invalid() {
        bindProperty(bindPropertyTestClass, "integerObjectField", "invalid");
    }

    @Test
    public void bindProperty_long() {
        bindProperty(bindPropertyTestClass, "longField", "1234567890123");
        assertEquals(1234567890123L, bindPropertyTestClass.longField);
    }

    @Test
    public void bindProperty_Long() {
        bindProperty(bindPropertyTestClass, "longObjectField", "null");
        assertNull(bindPropertyTestClass.longObjectField);

        bindProperty(bindPropertyTestClass, "longObjectField", "1234567890123");
        assertEquals(new Long("1234567890123"), bindPropertyTestClass.longObjectField);
    }

    @Test(expected = BindException.class)
    public void bindProperty_Long_invalid() {
        bindProperty(bindPropertyTestClass, "longObjectField", "invalid");
    }

    @Test
    public void bindProperty_float() {
        bindProperty(bindPropertyTestClass, "floatField", "23.42");
        assertEquals(23.42f, bindPropertyTestClass.floatField, 0.01);
    }

    @Test
    public void bindProperty_Float() {
        bindProperty(bindPropertyTestClass, "floatObjectField", "null");
        assertNull(bindPropertyTestClass.floatObjectField);

        bindProperty(bindPropertyTestClass, "floatObjectField", "23.42");
        assertEquals(new Float(23.42f), bindPropertyTestClass.floatObjectField);
    }

    @Test(expected = BindException.class)
    public void bindProperty_Float_invalid() {
        bindProperty(bindPropertyTestClass, "floatObjectField", "invalid");
    }

    @Test
    public void bindProperty_double() {
        bindProperty(bindPropertyTestClass, "doubleField", "23420000000000.2342");
        assertEquals(23420000000000.2342d, bindPropertyTestClass.doubleField, 0.0001);
    }

    @Test
    public void bindProperty_Double() {
        bindProperty(bindPropertyTestClass, "doubleObjectField", "null");
        assertNull(bindPropertyTestClass.doubleObjectField);

        bindProperty(bindPropertyTestClass, "doubleObjectField", "23420000000000.2342");
        assertEquals(new Double(23420000000000.2342d), bindPropertyTestClass.doubleObjectField);
    }

    @Test(expected = BindException.class)
    public void bindProperty_Double_invalid() {
        bindProperty(bindPropertyTestClass, "doubleObjectField", "invalid");
    }

    @SuppressWarnings("unused")
    private class BindPropertyTestClass {

        public byte byteField;
        public Byte byteObjectField;

        public short shortField;
        public Short shortObjectField;

        public int intField;
        public Integer integerField;

        public long longField;
        public Long longObjectField;

        public float floatField;
        public Float floatObjectField;

        public double doubleField;
        public Double doubleObjectField;
    }
}
