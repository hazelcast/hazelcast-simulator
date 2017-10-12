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
package com.hazelcast.simulator.protocol.core;

import org.junit.Test;

import static com.hazelcast.simulator.protocol.core.SimulatorAddress.COORDINATOR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class ResponseTest {

    private final SimulatorAddress destination = new SimulatorAddress(AddressLevel.AGENT, 42, 0, 0);

    private Response response = new Response(23, destination);

    @Test
    public void testConstructor_withSimulatorMessage() {
        SimulatorMessage simulatorMessage = new SimulatorMessage(destination, COORDINATOR, 12345, null, null);
        response = new Response(simulatorMessage);

        assertEquals(12345, response.getMessageId());
        assertEquals(COORDINATOR, response.getDestination());
    }

    @Test
    public void testConstructor_withSimulatorMessage_withResponse() {
        SimulatorMessage simulatorMessage = new SimulatorMessage(destination, COORDINATOR, 21435, null, null);
        response = new Response(simulatorMessage, ResponseType.FAILURE_COORDINATOR_NOT_FOUND);

        assertEquals(21435, response.getMessageId());
        assertEquals(COORDINATOR, response.getDestination());
        assertEquals(1, response.size());
        assertEquals(ResponseType.FAILURE_COORDINATOR_NOT_FOUND, response.getFirstErrorResponseType());
    }

    @Test
    public void testConstructor_withResponse() {
        response = new Response(54321, destination, COORDINATOR, ResponseType.FAILURE_AGENT_NOT_FOUND);

        assertEquals(54321, response.getMessageId());
        assertEquals(destination, response.getDestination());
        assertEquals(1, response.size());
        assertEquals(ResponseType.FAILURE_AGENT_NOT_FOUND, response.getFirstErrorResponseType());
    }

    @Test
    public void testGetMessageId() {
        assertEquals(23, response.getMessageId());
    }

    @Test
    public void testGetDestination() {
        assertEquals(destination, response.getDestination());
    }

    @Test
    public void testSize() {
        assertEquals(0, response.size());

        response.addPart(COORDINATOR, ResponseType.SUCCESS);
        assertEquals(1, response.size());
    }

    @Test
    public void testEntrySet() {
        assertEquals(0, response.getParts().size());

        response.addPart(COORDINATOR, ResponseType.SUCCESS);
        assertEquals(1, response.getParts().size());
    }

    @Test
    public void testGetFirstErrorResponseType() {
        response.addPart(COORDINATOR, ResponseType.SUCCESS);
        assertEquals(ResponseType.SUCCESS, response.getFirstErrorResponseType());

        response.addPart(COORDINATOR, ResponseType.EXCEPTION_DURING_OPERATION_EXECUTION);
        assertEquals(ResponseType.EXCEPTION_DURING_OPERATION_EXECUTION, response.getFirstErrorResponseType());
    }

    @Test
    public void testToString() {
        assertNotNull(response.toString());
    }
}
