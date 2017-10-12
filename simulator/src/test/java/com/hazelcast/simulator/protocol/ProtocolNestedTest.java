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
package com.hazelcast.simulator.protocol;

import com.hazelcast.simulator.protocol.core.Response;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.operation.IntegrationTestOperation;
import com.hazelcast.simulator.protocol.operation.SimulatorOperation;
import com.hazelcast.simulator.utils.ThreadSpawner;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static com.hazelcast.simulator.TestEnvironmentUtils.setupFakeUserDir;
import static com.hazelcast.simulator.TestEnvironmentUtils.teardownFakeUserDir;
import static com.hazelcast.simulator.protocol.ProtocolUtil.DEFAULT_TEST_TIMEOUT_MILLIS;
import static com.hazelcast.simulator.protocol.ProtocolUtil.assertAllTargets;
import static com.hazelcast.simulator.protocol.ProtocolUtil.sendFromCoordinator;
import static com.hazelcast.simulator.protocol.ProtocolUtil.startSimulatorComponents;
import static com.hazelcast.simulator.protocol.ProtocolUtil.stopSimulatorComponents;
import static com.hazelcast.simulator.protocol.core.AddressLevel.AGENT;
import static com.hazelcast.simulator.protocol.core.AddressLevel.TEST;
import static com.hazelcast.simulator.protocol.core.AddressLevel.WORKER;
import static com.hazelcast.simulator.protocol.core.ResponseType.SUCCESS;
import static com.hazelcast.simulator.protocol.operation.IntegrationTestOperation.Type.DEEP_NESTED_ASYNC;
import static com.hazelcast.simulator.protocol.operation.IntegrationTestOperation.Type.DEEP_NESTED_SYNC;
import static com.hazelcast.simulator.protocol.operation.IntegrationTestOperation.Type.NESTED_ASYNC;
import static com.hazelcast.simulator.protocol.operation.IntegrationTestOperation.Type.NESTED_SYNC;

public class ProtocolNestedTest {

    private static final int CONCURRENCY_LEVEL = 5;

    private static final SimulatorAddress ALL_AGENTS = new SimulatorAddress(AGENT, 0, 0, 0);
    private static final SimulatorAddress ALL_WORKERS = new SimulatorAddress(WORKER, 0, 0, 0);
    private static final SimulatorAddress ALL_TESTS = new SimulatorAddress(TEST, 0, 0, 0);

    private static final IntegrationTestOperation NESTED_SYNC_OPERATION = new IntegrationTestOperation(NESTED_SYNC);
    private static final IntegrationTestOperation NESTED_ASYNC_OPERATION = new IntegrationTestOperation(NESTED_ASYNC);
    private static final IntegrationTestOperation DEEP_NESTED_SYNC_OPERATION = new IntegrationTestOperation(DEEP_NESTED_SYNC);
    private static final IntegrationTestOperation DEEP_NESTED_ASYNC_OPERATION = new IntegrationTestOperation(DEEP_NESTED_ASYNC);

    private static final Logger LOGGER = Logger.getLogger(ProtocolNestedTest.class);

    @Before
    public void before() {
        setupFakeUserDir();
        startSimulatorComponents(2, 2, 2);
    }

    @After
    public void after() {
        stopSimulatorComponents();
        teardownFakeUserDir();
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT_MILLIS)
    public void nestedMessage_toAgent_syncWrite() {
        run("nestedMessage_toAgent_syncWrite", ALL_AGENTS, NESTED_SYNC_OPERATION, 2);
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT_MILLIS)
    public void nestedMessage_toAgent_asyncWrite() {
        run("nestedMessage_toAgent_syncWrite", ALL_AGENTS, NESTED_ASYNC_OPERATION, 2);
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT_MILLIS)
    public void nestedMessage_toWorker_syncWrite() {
        run("nestedMessage_toWorker_syncWrite", ALL_WORKERS, NESTED_SYNC_OPERATION, 4);
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT_MILLIS)
    public void nestedMessage_toWorker_asyncWrite() {
        run("nestedMessage_toWorker_asyncWrite", ALL_WORKERS, NESTED_ASYNC_OPERATION, 4);
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT_MILLIS)
    public void nestedMessage_toTest_syncWrite() {
        run("nestedMessage_toTest_syncWrite", ALL_TESTS, NESTED_SYNC_OPERATION, 8);
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT_MILLIS)
    public void nestedMessage_toTest_asyncWrite() {
        run("nestedMessage_toTest_asyncWrite", ALL_TESTS, NESTED_ASYNC_OPERATION, 8);
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT_MILLIS)
    public void deepNestedMessage_syncWrite() {
        run("deepNestedMessage_syncWrite", ALL_WORKERS, DEEP_NESTED_SYNC_OPERATION, 4);
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT_MILLIS)
    public void deepNestedMessage_asyncWrite() {
        run("deepNestedMessage_asyncWrite", ALL_WORKERS, DEEP_NESTED_ASYNC_OPERATION, 4);
    }

    private static void run(String spawnerName, final SimulatorAddress target, final SimulatorOperation operation,
                            final int expectedResponseCount) {
        ThreadSpawner spawner = new ThreadSpawner(spawnerName, true);
        for (int i = 0; i < CONCURRENCY_LEVEL; i++) {
            spawner.spawn(new Runnable() {
                @Override
                public void run() {
                    Response response = sendFromCoordinator(target, operation);

                    LOGGER.info("Response: " + response);
                    assertAllTargets(response, target, SUCCESS, expectedResponseCount);
                }
            });
        }
        spawner.awaitCompletion();
    }
}
