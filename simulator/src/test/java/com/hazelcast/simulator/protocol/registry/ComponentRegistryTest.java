package com.hazelcast.simulator.protocol.registry;

import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class ComponentRegistryTest {

    private ComponentRegistry registry = new ComponentRegistry();

    @Test
    public void testAddAgent() throws Exception {
        assertEquals(0, registry.agentCount());

        registry.addAgent("192.168.0.1", "192.168.0.1");
        assertEquals(1, registry.agentCount());
    }

    @Test
    public void testRemoveAgent() throws Exception {
        registry.addAgent("192.168.0.1", "192.168.0.1");
        assertEquals(1, registry.agentCount());

        AgentData agentData = registry.getFirstAgent();
        registry.removeAgent(agentData);
        assertEquals(0, registry.agentCount());
    }

    @Test
    public void testGetAgents() throws Exception {
        registry.addAgent("192.168.0.1", "192.168.0.1");
        registry.addAgent("192.168.0.2", "192.168.0.2");

        assertEquals(2, registry.agentCount());
        assertEquals(2, registry.getAgents().size());
    }

    @Test
    public void testGetAgents_withCount() throws Exception {
        registry.addAgent("192.168.0.1", "192.168.0.1");
        registry.addAgent("192.168.0.2", "192.168.0.2");
        registry.addAgent("192.168.0.3", "192.168.0.3");
        assertEquals(3, registry.agentCount());

        List<AgentData> agents = registry.getAgents(1);
        assertEquals(1, agents.size());
        assertEquals("192.168.0.3", agents.get(0).getPublicAddress());
        assertEquals("192.168.0.3", agents.get(0).getPrivateAddress());

        agents = registry.getAgents(2);
        assertEquals(2, agents.size());
        assertEquals("192.168.0.2", agents.get(0).getPublicAddress());
        assertEquals("192.168.0.2", agents.get(0).getPrivateAddress());
        assertEquals("192.168.0.3", agents.get(1).getPublicAddress());
        assertEquals("192.168.0.3", agents.get(1).getPrivateAddress());
    }
}
