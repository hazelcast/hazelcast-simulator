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
package com.hazelcast.simulator.tests;

import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.annotations.Run;
import com.hazelcast.simulator.test.annotations.Setup;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TestContextTest {

    private TestContext testContext;

    @Setup
    public void setUp(TestContext testContext) {
        this.testContext = testContext;
    }

    @Run
    void run() {
        assertNotNull(testContext.getTargetInstance());
        assertNotNull(testContext.getTestId());
        assertEquals(TestContext.LOCALHOST, testContext.getPublicIpAddress());

        testContext.stop();
        assertTrue(testContext.isStopped());
    }

    public TestContext getTestContext() {
        return testContext;
    }
}
