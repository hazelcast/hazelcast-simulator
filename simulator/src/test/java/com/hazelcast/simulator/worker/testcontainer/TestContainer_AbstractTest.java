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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.simulator.common.TestCase;
import com.hazelcast.simulator.protocol.connector.WorkerConnector;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.annotations.Run;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.utils.ExceptionReporter;
import org.junit.After;
import org.junit.Before;

import static com.hazelcast.simulator.TestEnvironmentUtils.setupFakeUserDir;
import static com.hazelcast.simulator.TestEnvironmentUtils.teardownFakeUserDir;
import static org.mockito.Mockito.mock;

public abstract class TestContainer_AbstractTest {

    TestContextImpl testContext = new TestContextImpl(
            mock(HazelcastInstance.class), "TestContainerTest","localhost",mock(WorkerConnector.class));

    TestContainer testContainer;

    @Before
    public void before() {
        ExceptionReporter.reset();
        setupFakeUserDir();
    }

    @After
    public void after() {
        teardownFakeUserDir();
    }

    <T> TestContainer createTestContainer(T test) {
        return new TestContainer(testContext, test, new TestCase("foo"));
    }

    <T> TestContainer createTestContainer(T test, TestCase testCase) {
        return new TestContainer(testContext, test, testCase);
    }

    public static class BaseTest {

        boolean runCalled;

        @Run
        public void run() {
            runCalled = true;
        }
    }

    public static class BaseSetupTest extends BaseTest {

        TestContext context;
        boolean setupCalled;

        @Setup
        public void setUp(TestContext context) {
            this.context = context;
            this.setupCalled = true;
        }
    }
}
