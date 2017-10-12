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

import com.hazelcast.simulator.common.TestCase;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.test.TestException;
import org.junit.Before;
import org.junit.Test;

import static com.hazelcast.simulator.common.FailureType.WORKER_EXCEPTION;
import static com.hazelcast.simulator.protocol.core.AddressLevel.AGENT;
import static com.hazelcast.simulator.protocol.core.AddressLevel.WORKER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class FailureOperationTest {

    private static final String TEST_ID = "ExceptionOperationTest";

    private SimulatorAddress workerAddress = new SimulatorAddress(WORKER, 1, 1, 0);
    private SimulatorAddress agentAddress = new SimulatorAddress(AGENT, 2, 0, 0);

    private TestException cause;

    private FailureOperation operation;
    private FailureOperation fullOperation;

    @Before
    public void before() {
        TestCase testCase = new TestCase(TEST_ID);
        cause = new TestException("expected exception");
        operation = new FailureOperation("FailureOperationTest", WORKER_EXCEPTION, workerAddress, null, cause);
        fullOperation = new FailureOperation("FailureOperationTest", WORKER_EXCEPTION, workerAddress, null, "127.0.0.1:5701",
                "C_A1_W1-member", TEST_ID, null).setTestCase(testCase);
    }

    @Test
    public void testGetType() {
        assertEquals(WORKER_EXCEPTION, operation.getType());
    }

    @Test
    public void testGetWorkerAddress() {
        assertEquals(workerAddress, operation.getWorkerAddress());
    }

    @Test
    public void testGetWorkerAddress_whenWorkerAddressIsNull() {
        operation = new FailureOperation("FailureOperationTest", WORKER_EXCEPTION, null, null, cause);

        assertNull(operation.getWorkerAddress());
    }

    @Test
    public void testGetTestId() {
        assertNull(operation.getTestId());
        assertEquals(TEST_ID, fullOperation.getTestId());
    }

    @Test
    public void testGetCause() {
        assertTrue(operation.getCause().contains("TestException"));
        assertNull(fullOperation.getCause());
    }

    @Test
    public void testGetLogMessage() {
        String log = fullOperation.getLogMessage(5);

        assertNotNull(log);
        assertTrue(log.contains("Failure #5"));
        assertTrue(log.contains(workerAddress.toString()));
        assertTrue(log.contains(TEST_ID));
    }

    @Test
    public void testGetLogMessage_whenWorkerAddressIsNull() {
        operation = new FailureOperation("FailureOperationTest", WORKER_EXCEPTION, null, agentAddress.toString(), cause);

        String log = operation.getLogMessage(5);

        assertNotNull(log);
        assertTrue(log.contains("Failure #5"));
        assertTrue(log.contains(agentAddress.toString()));
    }

    @Test
    public void testGetFileMessage_withTestCase() {
        String message = fullOperation.getFileMessage();

        assertNotNull(message);
        assertTrue(message.contains(workerAddress.toString()));
    }

    @Test
    public void testGetFileMessage_whenTestIdIsUnknown() {
        fullOperation.setTestCase(null);

        String message = fullOperation.getFileMessage();

        assertNotNull(message);
        assertTrue(message.contains(workerAddress.toString()));
        assertTrue(message.contains("test=" + TEST_ID + " (unknown)"));
    }

    @Test
    public void testGetFileMessage_whenTestIdIsNull() {
        String message = operation.getFileMessage();

        assertNotNull(message);
        assertTrue(message.contains(workerAddress.toString()));
        assertTrue(message.contains("test=null"));
    }
}
