/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.utils.BindException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TestContainer_NestedPropertyBindingTest extends TestContainer_AbstractTest {

    @Test
    public void testNestProperties() {
        TestCase testCase = new TestCase("id")
                .setProperty("nested.intField", 10)
                .setProperty("nested.booleanField", true)
                .setProperty("nested.stringField", "someString");

        DummyTest test = new DummyTest();

        testContainer = createTestContainer(test, testCase);

        assertNotNull(test.nested);
        assertEquals(10, test.nested.intField);
        assertEquals(true, test.nested.booleanField);
        assertEquals("someString", test.nested.stringField);
    }

    @Test(expected = BindException.class)
    public void testNestedPropertyNotFound() {
        TestCase testCase = new TestCase("id")
                .setProperty("nested.notExist", 10);

        DummyTest test = new DummyTest();
        createTestContainer(test, testCase);
    }

    @SuppressWarnings("WeakerAccess")
    public class DummyTest {

        public NestedProperties nested = new NestedProperties();

        @TimeStep
        public void timeStep() {
        }
    }

    @SuppressWarnings("WeakerAccess")
    public class NestedProperties {

        public int intField;
        public boolean booleanField;
        public String stringField;
    }
}
