package com.hazelcast.simulator.utils;

import org.junit.Test;

import static com.hazelcast.simulator.utils.FormatUtils.NEW_LINE;
import static com.hazelcast.simulator.utils.PropertyBindingSupport.bind0;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class PropertyBindingSupport_bind0_NumericalTest {

    private final TestObject testObject = new TestObject();

    @Test
    public void bind_byte() {
        boolean result = bind0(testObject, "byteField", "10");

        assertTrue(result);
        assertEquals(10, testObject.byteField);
    }

    @Test
    public void bind_byte_whitespace() {
        boolean result = bind0(testObject, "byteField", " 10 ");

        assertTrue(result);
        assertEquals(10, testObject.byteField);
    }

    @Test
    public void bind_Byte() {
        assertTrue(bind0(testObject, "byteObjectField", "null"));
        assertNull(testObject.byteObjectField);

        assertTrue(bind0(testObject, "byteObjectField", "10"));
        assertEquals(new Byte("10"), testObject.byteObjectField);
    }

    @Test(expected = BindException.class)
    public void bind_Byte_invalid() {
        bind0(testObject, "byteObjectField", "invalid");
    }

    @Test
    public void bind_short() {
        assertTrue(bind0(testObject, "shortField", "10"));
        assertEquals(10, testObject.shortField);
    }

    @Test
    public void bind_shortWithUnderscore() {
        assertTrue(bind0(testObject, "shortField", "10_000"));
        assertEquals(10000, testObject.shortField);
    }

    @Test
    public void bind_short_whitespace() {
        assertTrue(bind0(testObject, "shortField", " 10"));
        assertEquals(10, testObject.shortField);
    }

    @Test
    public void bind_Short() {
        assertTrue(bind0(testObject, "shortObjectField", "null"));
        assertNull(testObject.shortObjectField);

        assertTrue(bind0(testObject, "shortObjectField", "10"));
        assertEquals(new Short("10"), testObject.shortObjectField);
    }

    @Test(expected = BindException.class)
    public void bind_Short_invalid() {
        bind0(testObject, "shortObjectField", "invalid");
    }

    @Test
    public void bind_int() {
        assertTrue(bind0(testObject, "intField", "10"));
        assertEquals(10, testObject.intField);
    }

    @Test
    public void bind_intWithUnderscores() {
        assertTrue(bind0(testObject, "intField", "10_000_000"));
        assertEquals(10 * 1000 * 1000, testObject.intField);
    }

    @Test
    public void bind_int_whitespace() {
        assertTrue(bind0(testObject, "intField", "10" + NEW_LINE));
        assertEquals(10, testObject.intField);
    }

    @Test
    public void bind_Integer() {
        assertTrue(bind0(testObject, "integerField", "null"));
        assertNull(testObject.integerField);

        assertTrue(bind0(testObject, "integerField", "10"));
        assertEquals(new Integer(10), testObject.integerField);
    }

    @Test(expected = BindException.class)
    public void bind_Integer_invalid() {
        bind0(testObject, "integerField", "invalid");
    }

    @Test
    public void bind_long() {
        assertTrue(bind0(testObject, "longField", "1234567890123"));
        assertEquals(1234567890123L, testObject.longField);
    }

    @Test
    public void bind_longWithUnderScore() {
        assertTrue(bind0(testObject, "longField", "1_234_567_890_123"));
        assertEquals(1234567890123L, testObject.longField);
    }

    @Test
    public void bind_Long() {
        assertTrue(bind0(testObject, "longObjectField", "null"));
        assertNull(testObject.longObjectField);

        assertTrue(bind0(testObject, "longObjectField", "1234567890123"));
        assertEquals(new Long("1234567890123"), testObject.longObjectField);
    }

    @Test
    public void bind_LongWithUnderscore() {
        assertTrue(bind0(testObject, "longObjectField", "null"));
        assertNull(testObject.longObjectField);

        assertTrue(bind0(testObject, "longObjectField", "1_234_567_890_123"));
        assertEquals(new Long("1234567890123"), testObject.longObjectField);
    }

    @Test(expected = BindException.class)
    public void bind_Long_invalid() {
        bind0(testObject, "longObjectField", "invalid");
    }

    @Test
    public void bind_float() {
        assertTrue(bind0(testObject, "floatField", "23.42"));
        assertEquals(23.42f, testObject.floatField, 0.01);
    }

    @Test
    public void bind_Float() {
        assertTrue(bind0(testObject, "floatObjectField", "null"));
        assertNull(testObject.floatObjectField);

        assertTrue(bind0(testObject, "floatObjectField", "23.42"));
        assertEquals(new Float(23.42f), testObject.floatObjectField);
    }

    @Test(expected = BindException.class)
    public void bind_Float_invalid() {
        bind0(testObject, "floatObjectField", "invalid");
    }

    @Test
    public void bind_double() {
        assertTrue(bind0(testObject, "doubleField", "23420000000000.2342"));
        assertEquals(23420000000000.2342d, testObject.doubleField, 0.0001);
    }

    @Test
    public void bind_doubleWithUnderscore() {
        assertTrue(bind0(testObject, "doubleField", "23_420_000_000_000.2342"));
        assertEquals(23420000000000.2342d, testObject.doubleField, 0.0001);
    }

    @Test
    public void bind_Double() {
        assertTrue(bind0(testObject, "doubleObjectField", "null"));
        assertNull(testObject.doubleObjectField);

        assertTrue(bind0(testObject, "doubleObjectField", "23420000000000.2342"));
        assertEquals(new Double(23420000000000.2342d), testObject.doubleObjectField);
    }

    @Test(expected = BindException.class)
    public void bind_Double_invalid() {
        bind0(testObject, "doubleObjectField", "invalid");
    }

    @SuppressWarnings("unused")
    private class TestObject {

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
