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

import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.annotations.InjectTestContext;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class TestContainer_InjectTestContextTest extends TestContainer_AbstractTest {

    @Test
    public void testInjectTestContext() {
        TestContextTest test = new TestContextTest();
        testContainer = createTestContainer(test);

        assertNotNull(test.testContext);
        assertEquals(testContext, test.testContext);
    }

    @Test
    public void testInjectTestContext_withoutAnnotation() {
        TestContextTest test = new TestContextTest();
        testContainer = createTestContainer(test);

        assertNull(test.notAnnotatedTestContext);
    }

    private static class TestContextTest extends BaseTest {

        @InjectTestContext
        private TestContext testContext;

        @SuppressWarnings("unused")
        private TestContext notAnnotatedTestContext;
    }

    @Test(expected = IllegalTestException.class)
    public void testInjectTestContext_withIllegalFieldType() {
        IllegalFieldTypeTest test = new IllegalFieldTypeTest();
        testContainer = createTestContainer(test);
    }

    private static class IllegalFieldTypeTest extends BaseTest {

        @InjectTestContext
        private Object noProbeField;
    }
}
