package com.hazelcast.simulator.coordinator.registry;

import org.junit.Test;

import static com.hazelcast.simulator.protocol.core.SimulatorAddress.agentAddress;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

public class AgentDataTest {

    private static final int DEFAULT_ADDRESS_INDEX = 1;
    private static final String DEFAULT_PUBLIC_ADDRESS = "172.16.16.1";
    private static final String DEFAULT_PRIVATE_ADDRESS = "127.0.0.1";

    @Test
    public void testConstructor() {
        AgentData agentData = new AgentData(DEFAULT_ADDRESS_INDEX, DEFAULT_PUBLIC_ADDRESS, DEFAULT_PRIVATE_ADDRESS);

        assertEquals(agentAddress(DEFAULT_ADDRESS_INDEX), agentData.getAddress());
        assertEquals(DEFAULT_ADDRESS_INDEX, agentData.getAddressIndex());
        assertEquals(DEFAULT_PUBLIC_ADDRESS, agentData.getPublicAddress());
        assertEquals(DEFAULT_PRIVATE_ADDRESS, agentData.getPrivateAddress());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructor_addressIndexNegative() {
        new AgentData(-1, DEFAULT_PUBLIC_ADDRESS, DEFAULT_PRIVATE_ADDRESS);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructor_addressIndexZero() {
        new AgentData(0, DEFAULT_PUBLIC_ADDRESS, DEFAULT_PRIVATE_ADDRESS);
    }

    @Test(expected = NullPointerException.class)
    public void testConstructor_publicAddressNull() {
        new AgentData(DEFAULT_ADDRESS_INDEX, null, DEFAULT_PRIVATE_ADDRESS);
    }

    @Test(expected = NullPointerException.class)
    public void testConstructor_privateAddressNull() {
        new AgentData(DEFAULT_ADDRESS_INDEX, DEFAULT_PUBLIC_ADDRESS, null);
    }

    @Test
    public void testFormatIpAddresses_sameAddresses() {
        AgentData agentData = new AgentData(1, "192.168.0.1", "192.168.0.1");
        String ipAddresses = agentData.formatIpAddresses();
        assertTrue(ipAddresses.contains("192.168.0.1"));
    }

    @Test
    public void testFormatIpAddresses_differentAddresses() {
        AgentData agentData = new AgentData(1, "192.168.0.1", "172.16.16.1");
        String ipAddresses = agentData.formatIpAddresses();
        assertTrue(ipAddresses.contains("192.168.0.1"));
        assertTrue(ipAddresses.contains("172.16.16.1"));
    }

}
