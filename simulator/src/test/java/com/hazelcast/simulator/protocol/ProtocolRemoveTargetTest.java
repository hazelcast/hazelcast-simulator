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
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.hazelcast.simulator.TestEnvironmentUtils.resetLogLevel;
import static com.hazelcast.simulator.TestEnvironmentUtils.setLogLevel;
import static com.hazelcast.simulator.protocol.ProtocolUtil.DEFAULT_TEST_TIMEOUT_MILLIS;
import static com.hazelcast.simulator.protocol.ProtocolUtil.assertEmptyFutureMaps;
import static com.hazelcast.simulator.protocol.ProtocolUtil.assertSingleTarget;
import static com.hazelcast.simulator.protocol.ProtocolUtil.getAgentConnector;
import static com.hazelcast.simulator.protocol.ProtocolUtil.getCoordinatorConnector;
import static com.hazelcast.simulator.protocol.ProtocolUtil.getWorkerConnector;
import static com.hazelcast.simulator.protocol.ProtocolUtil.sendFromCoordinator;
import static com.hazelcast.simulator.protocol.ProtocolUtil.startSimulatorComponents;
import static com.hazelcast.simulator.protocol.ProtocolUtil.stopSimulatorComponents;
import static com.hazelcast.simulator.protocol.core.AddressLevel.AGENT;
import static com.hazelcast.simulator.protocol.core.AddressLevel.TEST;
import static com.hazelcast.simulator.protocol.core.AddressLevel.WORKER;
import static com.hazelcast.simulator.protocol.core.ResponseType.FAILURE_AGENT_NOT_FOUND;
import static com.hazelcast.simulator.protocol.core.ResponseType.FAILURE_TEST_NOT_FOUND;
import static com.hazelcast.simulator.protocol.core.ResponseType.FAILURE_WORKER_NOT_FOUND;
import static com.hazelcast.simulator.protocol.core.ResponseType.SUCCESS;

public class ProtocolRemoveTargetTest {

    @BeforeClass
    public static void beforeClass() {
        setLogLevel(Level.TRACE);

        startSimulatorComponents(1, 1, 1);
    }

    @AfterClass
    public static void afterClass() {
        stopSimulatorComponents();

        resetLogLevel();
    }

    @After
    public void commonAsserts() {
        assertEmptyFutureMaps();
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT_MILLIS)
    public void test() {
        SimulatorAddress testDestination = new SimulatorAddress(TEST, 1, 1, 1);

        Response response = sendFromCoordinator(testDestination);
        assertSingleTarget(response, testDestination, SUCCESS);

        getWorkerConnector(0).removeTest(1);
        response = sendFromCoordinator(testDestination);
        assertSingleTarget(response, testDestination.getParent(), FAILURE_TEST_NOT_FOUND);

        SimulatorAddress workerDestination = new SimulatorAddress(WORKER, 1, 1, 0);
        response = sendFromCoordinator(workerDestination);
        assertSingleTarget(response, workerDestination, SUCCESS);

        getAgentConnector(0).removeWorker(1);
        response = sendFromCoordinator(workerDestination);
        assertSingleTarget(response, workerDestination.getParent(), FAILURE_WORKER_NOT_FOUND);

        SimulatorAddress agentDestination = new SimulatorAddress(AGENT, 1, 0, 0);
        response = sendFromCoordinator(agentDestination);
        assertSingleTarget(response, agentDestination, SUCCESS);

        getCoordinatorConnector().removeAgent(1);
        response = sendFromCoordinator(agentDestination);
        assertSingleTarget(response, agentDestination.getParent(), FAILURE_AGENT_NOT_FOUND);
    }
}
