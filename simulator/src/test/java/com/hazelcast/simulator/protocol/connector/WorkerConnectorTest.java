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
package com.hazelcast.simulator.protocol.connector;

import com.hazelcast.simulator.common.WorkerType;
import com.hazelcast.simulator.protocol.core.AddressLevel;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.processors.WorkerOperationProcessor;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class WorkerConnectorTest {

    private static final int WORKER_INDEX = 1;
    private static final int AGENT_INDEX = 2;
    private static final int PORT = 11111;

    @Test
    public void testCreateInstance_withFileExceptionLogger() {
        WorkerConnector connector = new WorkerConnector(AGENT_INDEX, WORKER_INDEX, PORT, WorkerType.MEMBER, null, null);
        assertWorkerConnector(connector);
    }

    private void assertWorkerConnector(WorkerConnector connector) {
        SimulatorAddress address = connector.getAddress();
        assertEquals(AddressLevel.WORKER, address.getAddressLevel());
        assertEquals(WORKER_INDEX, address.getWorkerIndex());
        assertEquals(AGENT_INDEX, address.getAgentIndex());

        assertEquals(0, connector.getMessageQueueSize());
        assertEquals(PORT, connector.getPort());
        assertEquals(WorkerOperationProcessor.class, connector.getProcessor().getClass());
    }
}
