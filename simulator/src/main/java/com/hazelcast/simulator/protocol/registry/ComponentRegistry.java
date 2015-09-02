package com.hazelcast.simulator.protocol.registry;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Collections.synchronizedList;
import static java.util.Collections.unmodifiableList;

/**
 * Keeps track of all Simulator components which are running.
 *
 * Will be used by the Simulator Coordinator to translate all kind of selection queries to
 * {@link com.hazelcast.simulator.protocol.core.SimulatorAddress} to send messages to them.
 */
public class ComponentRegistry {

    private final AtomicInteger agentIndex = new AtomicInteger();
    private final List<AgentData> agents = synchronizedList(new ArrayList<AgentData>());

    public void addAgent(String publicAddress, String privateAddress) {
        AgentData agentData = new AgentData(agentIndex.incrementAndGet(), publicAddress, privateAddress);
        agents.add(agentData);
    }

    public void removeAgent(AgentData agentData) {
        agents.remove(agentData);
    }

    public int agentCount() {
        return agents.size();
    }

    public List<AgentData> getAgents() {
        return unmodifiableList(agents);
    }

    public List<AgentData> getAgents(int count) {
        int size = agents.size();
        return unmodifiableList(agents.subList(size - count, size));
    }

    public AgentData getFirstAgent() {
        return agents.get(0);
    }
}
