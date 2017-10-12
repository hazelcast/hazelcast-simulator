/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
