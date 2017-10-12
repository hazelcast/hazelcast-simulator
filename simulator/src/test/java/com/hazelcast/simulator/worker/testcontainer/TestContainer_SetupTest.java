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
import com.hazelcast.simulator.probes.Probe;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.annotations.Setup;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class TestContainer_SetupTest extends TestContainer_AbstractTest {

    @Test
    public void testSetup() throws Exception {
        BaseSetupTest test = new BaseSetupTest();
        testContainer = createTestContainer(test);
        testContainer.invoke(TestPhase.SETUP);

        assertTrue(test.setupCalled);
        assertSame(testContext, test.context);
        assertFalse(test.runCalled);
    }

    @Test
    public void testSetup_withoutArguments() {
        createTestContainer(new SetupWithoutArgumentsTest());
    }

    private static class SetupWithoutArgumentsTest extends BaseTest {

        @Setup
        public void setUp() {
        }
    }

    @Test
    public void testSetup_withValidArguments() {
        createTestContainer(new SetupWithValidArgumentsTest());
    }

    private static class SetupWithValidArgumentsTest extends BaseTest {

        @Setup
        public void setUp(TestContext testContext) {
        }
    }

    @Test(expected = IllegalTestException.class)
    public void testSetup_withInvalidArgument() {
        createTestContainer(new SetupWithInvalidArgument());
    }

    private static class SetupWithInvalidArgument extends BaseTest {

        @Setup
        public void setUp(Probe probe) {
        }
    }

    @Test(expected = IllegalTestException.class)
    public void testSetup_withObjectArgument() {
        createTestContainer(new SetupWithObjectArgument());
    }

    private static class SetupWithObjectArgument extends BaseTest {

        @Setup
        public void setUp(Object object) {
        }
    }

    @Test(expected = IllegalTestException.class)
    public void testSetup_withMixedSetupArguments() {
        createTestContainer(new MixedSetupArgumentsTest());
    }

    private static class MixedSetupArgumentsTest extends BaseTest {

        @Setup
        public void setUp(TestContext testContext, Object wrongType) {
        }
    }

    @Test(expected = IllegalTestException.class)
    public void testSetup_withDuplicateAnnotation() {
        createTestContainer(new DuplicateSetupAnnotationTest());
    }

    private static class DuplicateSetupAnnotationTest {

        @Setup
        public void setUp() {
        }

        @Setup
        public void anotherSetup() {
        }
    }
}
