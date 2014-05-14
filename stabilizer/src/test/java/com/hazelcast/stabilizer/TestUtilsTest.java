package com.hazelcast.stabilizer;

import com.hazelcast.stabilizer.tests.BindException;
import com.hazelcast.stabilizer.tests.TestUtils;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TestUtilsTest {

    @Test
    public void bindProperty_string() throws IllegalAccessException {
        SomeObject someObject = new SomeObject();

        TestUtils.bindProperty(someObject, "stringField", "null");
        assertNull(someObject.stringField);

        TestUtils.bindProperty(someObject, "stringField", "foo");
        assertEquals(someObject.stringField, "foo");
    }

    @Test
    public void bindProperty_enum() throws IllegalAccessException {
        SomeObject someObject = new SomeObject();

        TestUtils.bindProperty(someObject, "enumField", "null");
        assertNull(someObject.enumField);

        TestUtils.bindProperty(someObject, "enumField", TimeUnit.HOURS.name());
        assertEquals(someObject.enumField, TimeUnit.HOURS);
    }

    @Test
    public void bindProperty_int() throws IllegalAccessException {
        SomeObject someObject = new SomeObject();

        TestUtils.bindProperty(someObject, "intField", "10");
        assertEquals(someObject.intField,10);
    }

    @Test
    public void bindProperty_Integer() throws IllegalAccessException {
        SomeObject someObject = new SomeObject();

        TestUtils.bindProperty(someObject, "integerField", "null");
        assertNull(someObject.integerField);

        TestUtils.bindProperty(someObject, "integerField", "10");
        assertEquals(someObject.integerField, new Integer(10));
    }

    @Test(expected = BindException.class)
    public void bindProperty_unknownField() throws IllegalAccessException {
        SomeObject someObject = new SomeObject();

        TestUtils.bindProperty(someObject, "don'tknow", "null");
    }

    @Test(expected = BindException.class)
    public void bindProperty_unhandeledType() throws IllegalAccessException {
        SomeObject someObject = new SomeObject();

        TestUtils.bindProperty(someObject, "objectField", "null");
    }

    class SomeObject {
        private String stringField;
        private TimeUnit enumField;
        private int intField;
        private Integer integerField;
        private Object objectField;
    }

    public static File writeToTempFile(String text) throws IOException {
        File file = File.createTempFile("test", "test");
        file.deleteOnExit();
        Utils.writeText(text, file);
        return file;
    }
}
