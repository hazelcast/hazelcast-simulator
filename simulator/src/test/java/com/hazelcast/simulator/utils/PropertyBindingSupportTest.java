package com.hazelcast.simulator.utils;

import com.hazelcast.simulator.test.TestCase;
import org.junit.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static com.hazelcast.simulator.utils.PropertyBindingSupport.bindOptionalProperty;
import static com.hazelcast.simulator.utils.PropertyBindingSupport.bindProperties;
import static com.hazelcast.simulator.utils.PropertyBindingSupport.bindProperty;
import static com.hazelcast.simulator.utils.ReflectionUtils.invokePrivateConstructor;
import static org.junit.Assert.assertEquals;

public class PropertyBindingSupportTest {

    private final TestCase testCase = new TestCase("PropertyBindingSupportTest");
    private final BindPropertyTestClass bindPropertyTestClass = new BindPropertyTestClass();

    @Test
    public void testConstructor() throws Exception {
        invokePrivateConstructor(PropertyBindingSupport.class);
    }

    @Test
    public void bindProperties_notFound_optional() {
        testCase.setProperty("class", "willBeIgnored");
        testCase.setProperty("notExist", "isOptional");

        Set<String> optionalProperties = new HashSet<String>();
        optionalProperties.add("notExist");

        bindProperties(bindPropertyTestClass, testCase, optionalProperties);
    }

    @Test
    public void testBindOptionalProperty_testcaseIsNull() {
        bindOptionalProperty(bindPropertyTestClass, null, "ignored");
    }

    @Test
    public void testBindOptionalProperty_propertyNotDefined() {
        testCase.setProperty("class", "willBeIgnored");
        testCase.setProperty("notExist", "isOptional");

        bindOptionalProperty(bindPropertyTestClass, testCase, "propertyNotDefined");
    }

    @Test
    public void testBindOptionalProperty_propertyNotFound() {
        testCase.setProperty("class", "willBeIgnored");
        testCase.setProperty("notExist", "isOptional");

        bindOptionalProperty(bindPropertyTestClass, testCase, "notExist");
    }

    @Test
    public void testBindOptionalProperty() {
        testCase.setProperty("class", "willBeIgnored");
        testCase.setProperty("stringField", "foo");

        bindOptionalProperty(bindPropertyTestClass, testCase, "stringField");
        assertEquals("foo", bindPropertyTestClass.stringField);
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
    public void testFailFastWhenPropertyNotFound() {
        testCase.setProperty("doesNotExist", "foobar");
        bindProperties(bindPropertyTestClass, testCase, Collections.<String>emptySet());
    }

    @Test(expected = BindException.class)
    public void bindProperty_unknownField() {
        bindProperty(bindPropertyTestClass, "notExist", "null");
    }

    @Test(expected = BindException.class)
    public void bindProperty_protectedField() {
        bindProperty(bindPropertyTestClass, "protectedField", "newValue");
    }

    @Test(expected = BindException.class)
    public void bindProperty_packagePrivateField() {
        bindProperty(bindPropertyTestClass, "packagePrivateField", "newValue");
    }

    @Test(expected = BindException.class)
    public void bindProperty_privateField() {
        bindProperty(bindPropertyTestClass.otherObject, "privateField", "newValue");
    }

    @Test(expected = BindException.class)
    public void bindProperty_staticField() {
        bindProperty(bindPropertyTestClass.otherObject, "staticField", "newValue");
    }

    @Test(expected = BindException.class)
    public void bindProperty_finalField() {
        bindProperty(bindPropertyTestClass, "finalField", "newValue");
    }

    @Test(expected = BindException.class)
    public void bindProperty_fallsThroughAllChecks() {
        bindProperty(bindPropertyTestClass, "comparableField", "newValue");
    }

    @SuppressWarnings("unused")
    private class BindPropertyTestClass {

        public final int finalField = 5;

        public String stringField;
        public Comparable comparableField;

        public OtherObject otherObject = new OtherObject();
        public OtherObject nullOtherObject;

        protected String protectedField;
        String packagePrivateField;
        private String privateField;
    }

    @SuppressWarnings("unused")
    private static class OtherObject {

        public static Object staticField;

        public String stringField;
    }
}
