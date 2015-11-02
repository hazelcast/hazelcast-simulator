/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.simulator.protocol.core;

import com.hazelcast.simulator.protocol.operation.SimulatorOperation;
import com.hazelcast.simulator.protocol.processors.OperationProcessor;
import com.hazelcast.simulator.protocol.processors.TestOperationProcessor;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.simulator.protocol.core.AddressLevel.TEST;
import static com.hazelcast.simulator.protocol.core.ResponseType.FAILURE_TEST_NOT_FOUND;

/**
 * Manages {@link OperationProcessor} and {@link SimulatorAddress} instances for tests.
 */
public class TestProcessorManager {

    private final ConcurrentMap<Integer, SimulatorAddress> testAddresses = new ConcurrentHashMap<Integer, SimulatorAddress>();
    private final ConcurrentMap<Integer, TestOperationProcessor> testProcessors
            = new ConcurrentHashMap<Integer, TestOperationProcessor>();

    private final SimulatorAddress localAddress;
    private final int agentIndex;
    private final int workerIndex;

    public TestProcessorManager(SimulatorAddress localAddress) {
        this.localAddress = localAddress;
        this.agentIndex = localAddress.getAgentIndex();
        this.workerIndex = localAddress.getWorkerIndex();
    }

    public TestOperationProcessor getTest(int testIndex) {
        return testProcessors.get(testIndex);
    }

    public void addTest(int testIndex, TestOperationProcessor processor) {
        SimulatorAddress testAddress = new SimulatorAddress(TEST, agentIndex, workerIndex, testIndex);
        testAddresses.put(testIndex, testAddress);
        testProcessors.put(testIndex, processor);
    }

    public void removeTest(int testIndex) {
        testAddresses.remove(testIndex);
        testProcessors.remove(testIndex);
    }

    public void processOnAllTests(Response response, SimulatorOperation operation, SimulatorAddress source) {
        for (Map.Entry<Integer, TestOperationProcessor> entry : testProcessors.entrySet()) {
            ResponseType responseType = entry.getValue().process(operation, source);
            response.addResponse(testAddresses.get(entry.getKey()), responseType);
        }
    }

    public void processOnTest(Response response, SimulatorOperation operation, SimulatorAddress source, int testAddressIndex) {
        OperationProcessor processor = testProcessors.get(testAddressIndex);
        if (processor == null) {
            response.addResponse(localAddress, FAILURE_TEST_NOT_FOUND);
        } else {
            ResponseType responseType = processor.process(operation, source);
            response.addResponse(testAddresses.get(testAddressIndex), responseType);
        }
    }
}
