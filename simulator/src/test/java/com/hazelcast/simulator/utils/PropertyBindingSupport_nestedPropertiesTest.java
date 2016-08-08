package com.hazelcast.simulator.utils;

import com.hazelcast.simulator.common.TestCase;
import org.junit.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static com.hazelcast.simulator.utils.PropertyBindingSupport.bindAll;
import static junit.framework.TestCase.assertNotNull;
import static org.junit.Assert.assertEquals;

public class PropertyBindingSupport_nestedPropertiesTest {

    @Test
    public void testBindAll() {
        TestCase testCase = new TestCase("id")
                .setProperty("arm.finger.length", 10)
                .setProperty("age", 40);

        Person person = new Person();
        Set<String> usedProperties = bindAll(person, testCase);

        assertEquals(40, person.age);
        assertEquals(10, person.arm.finger.length);
        assertEquals(asSet("arm.finger.length", "age"), usedProperties);
    }

    @Test
    public void testNotAllPropertiesBound() {
        TestCase testCase = new TestCase("id")
                .setProperty("foo", 1000)
                .setProperty("arm.finger.length", 10)
                .setProperty("age", 40);

        Person person = new Person();
        Set<String> usedProperties = bindAll(person, testCase);

        assertEquals(40, person.age);
        assertEquals(10, person.arm.finger.length);
        assertEquals(asSet("arm.finger.length", "age"), usedProperties);
    }

    @Test(expected = BindException.class)
    public void testBadMatch() {
        TestCase testCase = new TestCase("id")
                .setProperty("arm.house.length", 10);

        Person person = new Person();
        bindAll(person, testCase);
    }

    @Test
    public void testReconstructObjectGraph() {
        TestCase testCase = new TestCase("id")
                .setProperty("nullArm.finger.length", 10);

        Person person = new Person();
        Set<String> usedProperties = bindAll(person, testCase);

        assertNotNull(person.nullArm);
        assertNotNull(person.nullArm.finger);
        assertEquals(10, person.nullArm.finger.length);
        assertEquals(asSet("nullArm.finger.length"), usedProperties);
    }

    private static Set<String> asSet(String... strings) {
        Set<String> result = new HashSet<String>();
        Collections.addAll(result, strings);
        return result;
    }

    private static class Person {

        public Arm arm = new Arm();
        public Arm nullArm;
        public int age;
    }

    static class Arm {

        public Finger finger = new Finger();
    }

    static class Finger {

        public int length;
    }
}
