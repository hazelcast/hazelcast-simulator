package com.hazelcast.simulator.utils;

import com.hazelcast.simulator.probes.probes.ProbesConfiguration;
import com.hazelcast.simulator.test.TestCase;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static com.hazelcast.simulator.utils.PropertyBindingSupport.bindOptionalProperty;
import static com.hazelcast.simulator.utils.PropertyBindingSupport.bindProperties;
import static com.hazelcast.simulator.utils.PropertyBindingSupport.parseProbeConfiguration;
import static com.hazelcast.simulator.utils.ReflectionUtils.invokePrivateConstructor;
import static org.junit.Assert.assertEquals;

public class PropertyBindingSupportTest {

    private final BindPropertyTestClass bindPropertyTestClass = new BindPropertyTestClass();

    @Test
    public void testConstructor() throws Exception {
        invokePrivateConstructor(PropertyBindingSupport.class);
    }

    @Test
    public void bindProperties_notFound_optional() throws NoSuchFieldException, IllegalAccessException {
        TestCase testCase = new TestCase();
        testCase.setProperty("class", "willBeIgnored");
        testCase.setProperty("probe-getLatency", "willBeIgnored");
        testCase.setProperty("notExist", "isOptional");

        Set<String> optionalProperties = new HashSet<String>();
        optionalProperties.add("notExist");

        bindProperties(bindPropertyTestClass, testCase, optionalProperties);
    }

    @Test
    public void testBindOptionalProperty_testcaseIsNull() throws NoSuchFieldException, IllegalAccessException {
        bindOptionalProperty(bindPropertyTestClass, null, "ignored");
    }

    @Test
    public void testBindOptionalProperty_propertyNotDefined() throws NoSuchFieldException, IllegalAccessException {
        TestCase testCase = new TestCase();
        testCase.setProperty("class", "willBeIgnored");
        testCase.setProperty("probe-getLatency", "willBeIgnored");
        testCase.setProperty("notExist", "isOptional");

        bindOptionalProperty(bindPropertyTestClass, testCase, "propertyNotDefined");
    }

    @Test
    public void testBindOptionalProperty_propertyNotFound() throws NoSuchFieldException, IllegalAccessException {
        TestCase testCase = new TestCase();
        testCase.setProperty("class", "willBeIgnored");
        testCase.setProperty("probe-getLatency", "willBeIgnored");
        testCase.setProperty("notExist", "isOptional");

        bindOptionalProperty(bindPropertyTestClass, testCase, "notExist");
    }

    @Test
    public void testBindOptionalProperty() throws NoSuchFieldException, IllegalAccessException {
        TestCase testCase = new TestCase();
        testCase.setProperty("class", "willBeIgnored");
        testCase.setProperty("probe-getLatency", "willBeIgnored");
        testCase.setProperty("stringField", "foo");

        bindOptionalProperty(bindPropertyTestClass, testCase, "stringField");
        assertEquals("foo", bindPropertyTestClass.stringField);
    }

    @Test
    public void testParseProbesConfiguration() {
        TestCase testCase = new TestCase();
        testCase.setProperty("class", "foobar");
        testCase.setProperty("probe-probe1", "latency");
        testCase.setProperty("probe-probe2", "hdr");

        ProbesConfiguration config = parseProbeConfiguration(testCase);
        assertEquals("latency", config.getConfig("probe1"));
        assertEquals("hdr", config.getConfig("probe2"));
        assertEquals(null, config.getConfig("notConfigured"));
    }

    @SuppressWarnings("unused")
    private class BindPropertyTestClass {

        private String stringField;
    }
}
