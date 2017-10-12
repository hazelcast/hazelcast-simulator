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

import com.hazelcast.simulator.common.TestPhase;
import com.hazelcast.simulator.test.annotations.Teardown;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestContainer_TeardownTest extends TestContainer_AbstractTest {

    @Test
    public void testLocalTeardown() throws Exception {
        TeardownTest test = new TeardownTest();
        testContainer = createTestContainer(test);
        testContainer.invoke(TestPhase.LOCAL_TEARDOWN);

        assertTrue(test.localTeardownCalled);
        assertFalse(test.globalTeardownCalled);
    }

    @Test
    public void testGlobalTeardown() throws Exception {
        TeardownTest test = new TeardownTest();
        testContainer = createTestContainer(test);
        testContainer.invoke(TestPhase.GLOBAL_TEARDOWN);

        assertFalse(test.localTeardownCalled);
        assertTrue(test.globalTeardownCalled);
    }

    private static class TeardownTest extends BaseTest {

        private boolean localTeardownCalled;
        private boolean globalTeardownCalled;

        @Teardown
        public void localTeardown() {
            localTeardownCalled = true;
        }

        @Teardown(global = true)
        public void globalTeardown() {
            globalTeardownCalled = true;
        }
    }
}
