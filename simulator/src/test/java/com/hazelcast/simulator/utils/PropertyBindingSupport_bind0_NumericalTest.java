package com.hazelcast.simulator.utils;

import org.junit.Test;

import static com.hazelcast.simulator.utils.FormatUtils.NEW_LINE;
import static com.hazelcast.simulator.utils.PropertyBindingSupport.bind0;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class PropertyBindingSupport_bind0_NumericalTest {

    private final BindPropertyTestClass bindPropertyTestClass = new BindPropertyTestClass();

    @Test
    public void bindProperty_byte() {
        bind0(bindPropertyTestClass, "byteField", "10");
        assertEquals(10, bindPropertyTestClass.byteField);
    }

    @Test
    public void bindProperty_byte_whitespace() {
        bind0(bindPropertyTestClass, "byteField", " 10 ");
        assertEquals(10, bindPropertyTestClass.byteField);
    }

    @Test
    public void bindProperty_Byte() {
        bind0(bindPropertyTestClass, "byteObjectField", "null");
        assertNull(bindPropertyTestClass.byteObjectField);

        bind0(bindPropertyTestClass, "byteObjectField", "10");
        assertEquals(new Byte("10"), bindPropertyTestClass.byteObjectField);
    }

    @Test(expected = BindException.class)
    public void bindProperty_Byte_invalid() {
        bind0(bindPropertyTestClass, "byteObjectField", "invalid");
    }

    @Test
    public void bindProperty_short() {
        bind0(bindPropertyTestClass, "shortField", "10");
        assertEquals(10, bindPropertyTestClass.shortField);
    }

    @Test
    public void bindProperty_short_whitespace() {
        bind0(bindPropertyTestClass, "shortField", " 10");
        assertEquals(10, bindPropertyTestClass.shortField);
    }

    @Test
    public void bindProperty_Short() {
        bind0(bindPropertyTestClass, "shortObjectField", "null");
        assertNull(bindPropertyTestClass.shortObjectField);

        bind0(bindPropertyTestClass, "shortObjectField", "10");
        assertEquals(new Short("10"), bindPropertyTestClass.shortObjectField);
    }

    @Test(expected = BindException.class)
    public void bindProperty_Short_invalid() {
        bind0(bindPropertyTestClass, "shortObjectField", "invalid");
    }

    @Test
    public void bindProperty_int() {
        bind0(bindPropertyTestClass, "intField", "10");
        assertEquals(10, bindPropertyTestClass.intField);
    }

    @Test
    public void bindProperty_int_whitespace() {
        bind0(bindPropertyTestClass, "intField", "10" + NEW_LINE);
        assertEquals(10, bindPropertyTestClass.intField);
    }

    @Test
    public void bindProperty_Integer() {
        bind0(bindPropertyTestClass, "integerField", "null");
        assertNull(bindPropertyTestClass.integerField);

        bind0(bindPropertyTestClass, "integerField", "10");
        assertEquals(new Integer(10), bindPropertyTestClass.integerField);
    }

    @Test(expected = BindException.class)
    public void bindProperty_Integer_invalid() {
        bind0(bindPropertyTestClass, "integerObjectField", "invalid");
    }

    @Test
    public void bindProperty_long() {
        bind0(bindPropertyTestClass, "longField", "1234567890123");
        assertEquals(1234567890123L, bindPropertyTestClass.longField);
    }

    @Test
    public void bindProperty_Long() {
        bind0(bindPropertyTestClass, "longObjectField", "null");
        assertNull(bindPropertyTestClass.longObjectField);

        bind0(bindPropertyTestClass, "longObjectField", "1234567890123");
        assertEquals(new Long("1234567890123"), bindPropertyTestClass.longObjectField);
    }

    @Test(expected = BindException.class)
    public void bindProperty_Long_invalid() {
        bind0(bindPropertyTestClass, "longObjectField", "invalid");
    }

    @Test
    public void bindProperty_float() {
        bind0(bindPropertyTestClass, "floatField", "23.42");
        assertEquals(23.42f, bindPropertyTestClass.floatField, 0.01);
    }

    @Test
    public void bindProperty_Float() {
        bind0(bindPropertyTestClass, "floatObjectField", "null");
        assertNull(bindPropertyTestClass.floatObjectField);

        bind0(bindPropertyTestClass, "floatObjectField", "23.42");
        assertEquals(new Float(23.42f), bindPropertyTestClass.floatObjectField);
    }

    @Test(expected = BindException.class)
    public void bindProperty_Float_invalid() {
        bind0(bindPropertyTestClass, "floatObjectField", "invalid");
    }

    @Test
    public void bindProperty_double() {
        bind0(bindPropertyTestClass, "doubleField", "23420000000000.2342");
        assertEquals(23420000000000.2342d, bindPropertyTestClass.doubleField, 0.0001);
    }

    @Test
    public void bindProperty_Double() {
        bind0(bindPropertyTestClass, "doubleObjectField", "null");
        assertNull(bindPropertyTestClass.doubleObjectField);

        bind0(bindPropertyTestClass, "doubleObjectField", "23420000000000.2342");
        assertEquals(new Double(23420000000000.2342d), bindPropertyTestClass.doubleObjectField);
    }

    @Test(expected = BindException.class)
    public void bindProperty_Double_invalid() {
        bind0(bindPropertyTestClass, "doubleObjectField", "invalid");
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
