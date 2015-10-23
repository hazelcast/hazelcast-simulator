package com.hazelcast.simulator.protocol.registry;

import com.hazelcast.simulator.agent.workerjvm.WorkerJvmSettings;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.utils.CommandLineExitException;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Collections.synchronizedList;
import static java.util.Collections.unmodifiableList;

/**
 * Keeps track of all Simulator components which are running.
 */
public class ComponentRegistry {

    private final AtomicInteger agentIndex = new AtomicInteger();
    private final List<AgentData> agents = synchronizedList(new ArrayList<AgentData>());
    private final List<WorkerData> workers = synchronizedList(new ArrayList<WorkerData>());

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
        if (agents.size() == 0) {
            throw new CommandLineExitException("No agents running!");
        }
        return agents.get(0);
    }

    public void addWorkers(SimulatorAddress parentAddress, List<WorkerJvmSettings> settingsList) {
        for (WorkerJvmSettings settings : settingsList) {
            WorkerData workerData = new WorkerData(parentAddress, settings);
            workers.add(workerData);
        }
    }

    public void removeWorker(SimulatorAddress workerAddress) {
        Iterator<WorkerData> iterator = workers.iterator();
        while (iterator.hasNext()) {
            WorkerData workerData = iterator.next();
            if (workerData.getAddress().equals(workerAddress)) {
                iterator.remove();
                break;
            }
        }
    }

    public void removeWorker(WorkerData workerData) {
        workers.remove(workerData);
    }

    public int workerCount() {
        return workers.size();
    }

    public List<WorkerData> getWorkers() {
        return unmodifiableList(workers);
    }

    public WorkerData getFirstWorker() {
        if (workers.size() == 0) {
            throw new CommandLineExitException("No workers running!");
        }
        return workers.get(0);
    }

    public Set<SimulatorAddress> getMissingWorkers(Set<SimulatorAddress> finishedWorkers) {
        Set<SimulatorAddress> missingWorkers = new HashSet<SimulatorAddress>();
        for (WorkerData worker : workers) {
            SimulatorAddress workerAddress = worker.getAddress();
            if (!finishedWorkers.contains(workerAddress)) {
                missingWorkers.add(workerAddress);
            }
        }
        return missingWorkers;
    }
}
