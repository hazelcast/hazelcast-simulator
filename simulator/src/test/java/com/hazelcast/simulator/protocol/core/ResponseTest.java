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

        response.addResponse(COORDINATOR, ResponseType.SUCCESS);
        assertEquals(1, response.size());
    }

    @Test
    public void testEntrySet() {
        assertEquals(0, response.entrySet().size());

        response.addResponse(COORDINATOR, ResponseType.SUCCESS);
        assertEquals(1, response.entrySet().size());
    }

    @Test
    public void testGetFirstErrorResponseType() {
        response.addResponse(COORDINATOR, ResponseType.SUCCESS);
        assertEquals(ResponseType.SUCCESS, response.getFirstErrorResponseType());

        response.addResponse(COORDINATOR, ResponseType.EXCEPTION_DURING_OPERATION_EXECUTION);
        assertEquals(ResponseType.EXCEPTION_DURING_OPERATION_EXECUTION, response.getFirstErrorResponseType());
    }

    @Test
    public void testToString() {
        assertNotNull(response.toString());
    }
}
