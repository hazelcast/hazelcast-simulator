package com.hazelcast.simulator.common;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class AgentAddressTest {

    private static final String DEFAULT_PUBLIC_ADDRESS = "172.16.16.1";
    private static final String DEFAULT_PRIVATE_ADDRESS = "127.0.0.1";

    private AgentAddress agentAddress;

    @Test(expected = NullPointerException.class)
    public void testConstructor_publicAddressNull() {
        new AgentAddress(null, DEFAULT_PRIVATE_ADDRESS);
    }

    @Test(expected = NullPointerException.class)
    public void testConstructor_privateAddressNull() {
        new AgentAddress(DEFAULT_PUBLIC_ADDRESS, null);
    }

    @Test
    public void testConstructor() {
        agentAddress = new AgentAddress(DEFAULT_PUBLIC_ADDRESS, DEFAULT_PRIVATE_ADDRESS);
        assertEquals(DEFAULT_PUBLIC_ADDRESS, agentAddress.publicAddress);
        assertEquals(DEFAULT_PRIVATE_ADDRESS, agentAddress.privateAddress);
    }

    @Test
    @SuppressWarnings("all")
    public void testEquals_self() {
        agentAddress = new AgentAddress(DEFAULT_PUBLIC_ADDRESS, DEFAULT_PRIVATE_ADDRESS);
        assertTrue(agentAddress.equals(agentAddress));
        assertEquals(agentAddress.hashCode(), agentAddress.hashCode());
    }

    @Test
    @SuppressWarnings("all")
    public void testEquals_null() {
        agentAddress = new AgentAddress(DEFAULT_PUBLIC_ADDRESS, DEFAULT_PRIVATE_ADDRESS);
        assertFalse(agentAddress.equals(null));
    }

    @Test
    public void testEquals_differentClass() {
        agentAddress = new AgentAddress(DEFAULT_PUBLIC_ADDRESS, DEFAULT_PRIVATE_ADDRESS);
        assertFalse(agentAddress.equals(new Object()));
    }

    @Test
    public void testEquals_differentPublicIp() {
        agentAddress = new AgentAddress(DEFAULT_PUBLIC_ADDRESS, DEFAULT_PRIVATE_ADDRESS);
        AgentAddress other = new AgentAddress("different", DEFAULT_PRIVATE_ADDRESS);
        assertFalse(agentAddress.equals(other));
    }

    @Test
    public void testEquals_differentPrivateIp() {
        agentAddress = new AgentAddress(DEFAULT_PUBLIC_ADDRESS, DEFAULT_PRIVATE_ADDRESS);
        AgentAddress other = new AgentAddress(DEFAULT_PUBLIC_ADDRESS, "different");
        assertFalse(agentAddress.equals(other));
    }

    @Test
    public void testEquals() {
        agentAddress = new AgentAddress(DEFAULT_PUBLIC_ADDRESS, DEFAULT_PRIVATE_ADDRESS);
        AgentAddress other = new AgentAddress(DEFAULT_PUBLIC_ADDRESS, DEFAULT_PRIVATE_ADDRESS);
        assertTrue(agentAddress.equals(other));
    }

    @Test
    public void testToString() {
        agentAddress = new AgentAddress(DEFAULT_PUBLIC_ADDRESS, DEFAULT_PRIVATE_ADDRESS);
        assertNotNull(agentAddress.toString());
    }
}