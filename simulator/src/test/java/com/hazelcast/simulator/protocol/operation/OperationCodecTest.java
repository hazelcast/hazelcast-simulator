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
package com.hazelcast.simulator.protocol.operation;

import com.hazelcast.simulator.agent.workerprocess.WorkerProcessSettings;
import com.hazelcast.simulator.common.WorkerType;
import com.hazelcast.simulator.protocol.core.SimulatorMessage;
import org.apache.log4j.Logger;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.simulator.protocol.core.SimulatorAddress.COORDINATOR;
import static com.hazelcast.simulator.protocol.operation.IntegrationTestOperation.Type.EQUALS;
import static com.hazelcast.simulator.protocol.operation.OperationCodec.fromJson;
import static com.hazelcast.simulator.protocol.operation.OperationCodec.fromSimulatorMessage;
import static com.hazelcast.simulator.protocol.operation.OperationCodec.toJson;
import static com.hazelcast.simulator.utils.ReflectionUtils.invokePrivateConstructor;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class OperationCodecTest {

    private static final Logger LOGGER = Logger.getLogger(OperationCodecTest.class);

    @Test
    public void testConstructor() throws Exception {
        invokePrivateConstructor(OperationCodec.class);
    }

    @Test
    public void testCodec_fromJson() {
        IntegrationTestOperation operation = new IntegrationTestOperation(EQUALS, "codecTest_fromJson");
        String json = toJson(operation);
        assertNotNull(json);

        IntegrationTestOperation decoded = (IntegrationTestOperation) fromJson(json, IntegrationTestOperation.class);
        assertEquals(operation.getTestData(), decoded.getTestData());
    }

    @Test
    public void testCodec_fromSimulatorMessage() {
        IntegrationTestOperation operation = new IntegrationTestOperation(EQUALS, "codecTest_fromSimulatorMessage");
        String json = toJson(operation);
        assertNotNull(json);

        SimulatorMessage message = new SimulatorMessage(COORDINATOR, COORDINATOR, 1, OperationType.INTEGRATION_TEST, json);
        IntegrationTestOperation decoded = (IntegrationTestOperation) fromSimulatorMessage(message);
        assertEquals(operation.getTestData(), decoded.getTestData());
    }

    @Test
    public void testCodec_withComplexOperation() {
        Map<String, String> environment = new HashMap<String, String>();
        environment.put("FOO", "BAR");
        WorkerProcessSettings originalSettings = new WorkerProcessSettings(
                1, WorkerType.MEMBER, "outofthebox", "somescript", 10, environment);

        CreateWorkerOperation operation = new CreateWorkerOperation(singletonList(originalSettings), 0);
        String json = toJson(operation);

        assertNotNull(json);
        LOGGER.info(json);

        CreateWorkerOperation decoded = (CreateWorkerOperation) fromJson(json, CreateWorkerOperation.class);
        WorkerProcessSettings decodedSettings = decoded.getWorkerProcessSettings().get(0);
        assertEquals(originalSettings.getWorkerIndex(), decodedSettings.getWorkerIndex());
        assertEquals(originalSettings.getWorkerType(), decodedSettings.getWorkerType());
        assertEquals(originalSettings.getEnvironment(), decodedSettings.getEnvironment());
        assertEquals(originalSettings.getWorkerStartupTimeout(), decodedSettings.getWorkerStartupTimeout());
        assertEquals(originalSettings.getWorkerScript(), decodedSettings.getWorkerScript());
    }
}
