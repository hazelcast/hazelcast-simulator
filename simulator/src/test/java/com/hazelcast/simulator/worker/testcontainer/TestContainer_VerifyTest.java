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

import com.hazelcast.simulator.common.TestPhase;
import com.hazelcast.simulator.test.annotations.Verify;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestContainer_VerifyTest extends TestContainer_AbstractTest {

    @Test
    public void testLocalVerify() throws Exception {
        VerifyTest test = new VerifyTest();
        testContainer = createTestContainer(test);
        testContainer.invoke(TestPhase.LOCAL_VERIFY);

        assertTrue(test.localVerifyCalled);
        assertFalse(test.globalVerifyCalled);
    }

    @Test
    public void testGlobalVerify() throws Exception {
        VerifyTest test = new VerifyTest();
        testContainer = createTestContainer(test);
        testContainer.invoke(TestPhase.GLOBAL_VERIFY);

        assertFalse(test.localVerifyCalled);
        assertTrue(test.globalVerifyCalled);
    }

    private static class VerifyTest extends BaseTest {

        private boolean localVerifyCalled;
        private boolean globalVerifyCalled;

        @Verify(global = false)
        public void localVerify() {
            localVerifyCalled = true;
        }

        @Verify
        public void globalVerify() {
            globalVerifyCalled = true;
        }
    }
}
