package com.hazelcast.simulator.utils;

import com.hazelcast.simulator.probes.probes.IntervalProbe;
import com.hazelcast.simulator.probes.probes.ProbesConfiguration;
import com.hazelcast.simulator.test.TestCase;
import org.junit.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static com.hazelcast.simulator.utils.PropertyBindingSupport.bindOptionalProperty;
import static com.hazelcast.simulator.utils.PropertyBindingSupport.bindProperties;
import static com.hazelcast.simulator.utils.PropertyBindingSupport.parseProbeConfiguration;
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
        testCase.setProperty("probe-getLatency", "willBeIgnored");
        testCase.setProperty("notExist", "isOptional");

        bindOptionalProperty(bindPropertyTestClass, testCase, "propertyNotDefined");
    }

    @Test
    public void testBindOptionalProperty_propertyNotFound() {
        testCase.setProperty("class", "willBeIgnored");
        testCase.setProperty("probe-getLatency", "willBeIgnored");
        testCase.setProperty("notExist", "isOptional");

        bindOptionalProperty(bindPropertyTestClass, testCase, "notExist");
    }

    @Test
    public void testBindOptionalProperty() {
        testCase.setProperty("class", "willBeIgnored");
        testCase.setProperty("probe-getLatency", "willBeIgnored");
        testCase.setProperty("stringField", "foo");

        bindOptionalProperty(bindPropertyTestClass, testCase, "stringField");
        assertEquals("foo", bindPropertyTestClass.stringField);
    }

    @Test
    public void testParseProbesConfiguration() {
        testCase.setProperty("class", "foobar");
        testCase.setProperty("probe1", "latency");
        testCase.setProperty("probe2", "hdr");

        TestClassWithProbes testInstance = new TestClassWithProbes();

        ProbesConfiguration config = parseProbeConfiguration(testInstance, testCase);
        assertEquals("latency", config.getConfig("probe1"));
        assertEquals("hdr", config.getConfig("probe2"));
        assertEquals(null, config.getConfig("notConfigured"));
    }

    @Test(expected = BindException.class)
    public void testFailFastWhenPropertyNotFound() {
        testCase.setProperty("doesNotExist", "foobar");
        bindProperties(bindPropertyTestClass, testCase, Collections.EMPTY_SET);
    }

    @SuppressWarnings("unused")
    private class BindPropertyTestClass {

        public String stringField;
    }

    private class TestClassWithProbes {
        public IntervalProbe probe1;
        public IntervalProbe probe2;
    }
}
