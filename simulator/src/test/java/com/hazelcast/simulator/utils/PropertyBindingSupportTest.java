package com.hazelcast.simulator.utils;

import com.hazelcast.simulator.test.TestCase;
import org.junit.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static com.hazelcast.simulator.utils.PropertyBindingSupport.bind;
import static com.hazelcast.simulator.utils.PropertyBindingSupport.bind0;
import static com.hazelcast.simulator.utils.ReflectionUtils.invokePrivateConstructor;
import static jdk.nashorn.internal.objects.NativeObject.bindProperties;
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
        bind(bindPropertyTestClass, null, "ignored");
    }

    @Test
    public void testBindOptionalProperty_propertyNotDefined() {
        testCase.setProperty("class", "willBeIgnored");
        testCase.setProperty("notExist", "isOptional");

        bind(bindPropertyTestClass, testCase, "propertyNotDefined");
    }

    @Test
    public void testBindOptionalProperty_propertyNotFound() {
        testCase.setProperty("class", "willBeIgnored");
        testCase.setProperty("notExist", "isOptional");

        bind(bindPropertyTestClass, testCase, "notExist");
    }

    @Test
    public void testBindOptionalProperty() {
        testCase.setProperty("class", "willBeIgnored");
        testCase.setProperty("stringField", "foo");

        bind(bindPropertyTestClass, testCase, "stringField");
        assertEquals("foo", bindPropertyTestClass.stringField);
    }

    @Test
    public void bindProperty_withPath() {
        bind0(bindPropertyTestClass, "otherObject.stringField", "newValue");
        assertEquals("newValue", bindPropertyTestClass.otherObject.stringField);
    }

    @Test(expected = BindException.class)
    public void bindProperty_withPathAndNullValue() {
        bind0(bindPropertyTestClass, "nullOtherObject.stringField", "newValue");
    }

    @Test(expected = BindException.class)
    public void bindProperty_withPath_missingProperty() {
        bind0(bindPropertyTestClass, "notExist.stringField", "newValue");
    }

    @Test(expected = BindException.class)
    public void testFailFastWhenPropertyNotFound() {
        testCase.setProperty("doesNotExist", "foobar");
        bindProperties(bindPropertyTestClass, testCase, Collections.<String>emptySet());
    }

    @Test(expected = BindException.class)
    public void bindProperty_unknownField() {
        bind0(bindPropertyTestClass, "notExist", "null");
    }

    @Test(expected = BindException.class)
    public void bindProperty_protectedField() {
        bind0(bindPropertyTestClass, "protectedField", "newValue");
    }

    @Test(expected = BindException.class)
    public void bindProperty_packagePrivateField() {
        bind0(bindPropertyTestClass, "packagePrivateField", "newValue");
    }

    @Test(expected = BindException.class)
    public void bindProperty_privateField() {
        bind0(bindPropertyTestClass.otherObject, "privateField", "newValue");
    }

    @Test(expected = BindException.class)
    public void bindProperty_staticField() {
        bind0(bindPropertyTestClass.otherObject, "staticField", "newValue");
    }

    @Test(expected = BindException.class)
    public void bindProperty_finalField() {
        bind0(bindPropertyTestClass, "finalField", "newValue");
    }

    @Test(expected = BindException.class)
    public void bindProperty_fallsThroughAllChecks() {
        bind0(bindPropertyTestClass, "comparableField", "newValue");
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
