package com.hazelcast.stabilizer.tests.utils;

import com.hazelcast.stabilizer.Utils;
import com.hazelcast.stabilizer.tests.BindException;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.stabilizer.tests.utils.PropertyBindingSupport.bindProperty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class PropertyBindingSupportTest {

    @Test
    public void bindProperty_string() throws IllegalAccessException {
        SomeObject someObject = new SomeObject();

        bindProperty(someObject, "stringField", "null");
        assertNull(someObject.stringField);

        bindProperty(someObject, "stringField", "foo");
        assertEquals(someObject.stringField, "foo");
    }

    @Test
    public void bindProperty_enum_nullValue() throws IllegalAccessException {
        SomeObject someObject = new SomeObject();

        bindProperty(someObject, "enumField", "null");
        assertNull(someObject.enumField);
    }

    @Test
    public void bindProperty_enum() throws IllegalAccessException {
        SomeObject someObject = new SomeObject();

        bindProperty(someObject, "enumField", TimeUnit.HOURS.name());
        assertEquals(someObject.enumField, TimeUnit.HOURS);
    }

    @Test
    public void bindProperty_enum_caseInsensitive() throws IllegalAccessException {
        SomeObject someObject = new SomeObject();

        bindProperty(someObject, "enumField", "dAyS");
        assertEquals(someObject.enumField, TimeUnit.DAYS);
    }

    @Test(expected = BindException.class)
    public void bindProperty_enum_notFound() throws IllegalAccessException {
        SomeObject someObject = new SomeObject();

        bindProperty(someObject, "enumField", "dontexist");
    }

    @Test
    public void bindProperty_int() throws IllegalAccessException {
        SomeObject someObject = new SomeObject();

        bindProperty(someObject, "intField", "10");
        assertEquals(someObject.intField, 10);
    }

    @Test
    public void bindProperty_Integer() throws IllegalAccessException {
        SomeObject someObject = new SomeObject();

        bindProperty(someObject, "integerField", "null");
        assertNull(someObject.integerField);

        bindProperty(someObject, "integerField", "10");
        assertEquals(someObject.integerField, new Integer(10));
    }

    @Test(expected = BindException.class)
    public void bindProperty_unknownField() throws IllegalAccessException {
        SomeObject someObject = new SomeObject();

        bindProperty(someObject, "notexist", "null");
    }

    @Test(expected = BindException.class)
    public void bindProperty_unhandeledType() throws IllegalAccessException {
        SomeObject someObject = new SomeObject();

        bindProperty(someObject, "objectField", "null");
    }

    @Test
    public void bindProperty_withPath() throws IllegalAccessException {
        SomeObject someObject = new SomeObject();

        bindProperty(someObject, "otherObject.stringField", "newvalue");

        assertEquals("newvalue",someObject.otherObject.stringField);
    }

    @Test(expected = BindException.class)
    public void bindProperty_withPathAndNullValue() throws IllegalAccessException {
        SomeObject someObject = new SomeObject();

        bindProperty(someObject, "nullOtherObject.stringField", "newvalue");
    }

    @Test(expected = BindException.class)
    public void bindProperty_withPath_missingProperty() throws IllegalAccessException {
        SomeObject someObject = new SomeObject();

        bindProperty(someObject, "notexist.stringField", "newvalue");
    }

    class SomeObject {
        private String stringField;
        private TimeUnit enumField;
        private int intField;
        private Integer integerField;
        private Object objectField;
        public OtherObject otherObject = new OtherObject();
        public OtherObject nullOtherObject;
    }

    class OtherObject {
        public String stringField;

    }


    public static File writeToTempFile(String text) throws IOException {
        File file = File.createTempFile("test", "test");
        file.deleteOnExit();
        Utils.writeText(text, file);
        return file;
    }
}
