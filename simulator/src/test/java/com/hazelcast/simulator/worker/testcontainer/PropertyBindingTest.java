package com.hazelcast.simulator.worker.testcontainer;

import com.hazelcast.simulator.common.TestCase;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class PropertyBindingTest {
    @Test
    public void loadAsClass_nonExisting() {
        TestCase testCase = new TestCase("foo");
        PropertyBinding binding = new PropertyBinding(testCase);

        assertEquals(Long.class, binding.loadAsClass("classValue", Long.class));
    }

    @Test
    public void loadAsClass_existing() {
        TestCase testCase = new TestCase("foo")
                .setProperty("classValue", String.class);
        PropertyBinding binding = new PropertyBinding(testCase);

        assertEquals(String.class, binding.loadAsClass("classValue", Long.class));
    }

    @Test
    public void loadAsDouble_nonExisting() {
        TestCase testCase = new TestCase("foo");
        PropertyBinding binding = new PropertyBinding(testCase);

        assertEquals(10, binding.loadAsDouble("doubleValue", 10), 0.1);
    }

    @Test
    public void loadAsDouble_existing() {
        TestCase testCase = new TestCase("foo")
                .setProperty("doubleValue", 50d);
        PropertyBinding binding = new PropertyBinding(testCase);

        assertEquals(50, binding.loadAsDouble("doubleValue", 10), 0.1);
    }

    @Test
    public void loadAsDouble_existing_wihUnderscores() {
        TestCase testCase = new TestCase("foo")
                .setProperty("doubleValue", "5_0d");
        PropertyBinding binding = new PropertyBinding(testCase);

        assertEquals(50, binding.loadAsDouble("doubleValue", 10), 0.1);
    }

    @Test
    public void loadAsLong_nonExisting() {
        TestCase testCase = new TestCase("foo");
        PropertyBinding binding = new PropertyBinding(testCase);

        assertEquals(10, binding.loadAsLong("longValue", 10));
    }

    @Test
    public void loadAsLong_existing() {
        TestCase testCase = new TestCase("foo")
                .setProperty("longValue", 50);
        PropertyBinding binding = new PropertyBinding(testCase);

        assertEquals(50, binding.loadAsLong("longValue", 10));
    }

    @Test
    public void loadAsLong_existing_withUnderscores() {
        TestCase testCase = new TestCase("foo")
                .setProperty("longValue", "5_0");
        PropertyBinding binding = new PropertyBinding(testCase);

        assertEquals(50, binding.loadAsLong("longValue", 10));
    }

    @Test
    public void loadAsInt_nonExisting() {
        TestCase testCase = new TestCase("foo");
        PropertyBinding binding = new PropertyBinding(testCase);

        assertEquals(10, binding.loadAsInt("intValue", 10));
    }

    @Test
    public void loadAsInt_existing() {
        TestCase testCase = new TestCase("foo")
                .setProperty("intValue", 50);
        PropertyBinding binding = new PropertyBinding(testCase);

        assertEquals(50, binding.loadAsInt("intValue", 10));
    }

    @Test
    public void loadAsInt_existing_withUnderscores() {
        TestCase testCase = new TestCase("foo")
                .setProperty("intValue", "5_0");
        PropertyBinding binding = new PropertyBinding(testCase);

        assertEquals(50, binding.loadAsInt("intValue", 10));
    }

    @Test
    public void loadAsBoolean_nonExisting() {
        TestCase testCase = new TestCase("foo");
        PropertyBinding binding = new PropertyBinding(testCase);

        assertEquals(true, binding.loadAsBoolean("booleanValue", true));
    }

    @Test
    public void loadAsBoolean_existing() {
        TestCase testCase = new TestCase("foo")
                .setProperty("booleanValue", false);

        PropertyBinding binding = new PropertyBinding(testCase);

        assertEquals(false, binding.loadAsBoolean("booleanValue", true));
    }
}
