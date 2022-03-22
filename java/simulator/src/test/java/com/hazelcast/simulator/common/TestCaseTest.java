package com.hazelcast.simulator.common;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;


public class TestCaseTest {

    @Test
    public void constructor_propertyWithWhiteSpace() {
        Map<String, String> props = new HashMap<String, String>();
        props.put("x", " a ");
        TestCase testCase = new TestCase("id", props);

        assertEquals("a", testCase.getProperty("x"));
    }

    @Test
    public void setPropertyWithWhiteSpace() {
        TestCase testCase = new TestCase("id");
        testCase.setProperty("x", " a ");

        assertEquals("a", testCase.getProperty("x"));
    }
}
