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
package com.hazelcast.simulator.protocol.registry;

import com.hazelcast.simulator.agent.workerjvm.WorkerJvmSettings;
import com.hazelcast.simulator.protocol.core.AddressLevel;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.test.TestCase;
import com.hazelcast.simulator.test.TestSuite;
import com.hazelcast.simulator.utils.CommandLineExitException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.Math.ceil;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Collections.synchronizedList;
import static java.util.Collections.unmodifiableList;

/**
 * Keeps track of all Simulator components which are running.
 */
public class ComponentRegistry {

    private final AtomicInteger agentIndex = new AtomicInteger();
    private final AtomicInteger testIndex = new AtomicInteger();

    private final List<AgentData> agents = synchronizedList(new ArrayList<AgentData>());
    private final List<WorkerData> workers = synchronizedList(new ArrayList<WorkerData>());
    private final ConcurrentMap<String, TestData> tests = new ConcurrentHashMap<String, TestData>();

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

    public synchronized void addWorkers(SimulatorAddress parentAddress, List<WorkerJvmSettings> settingsList) {
        for (WorkerJvmSettings settings : settingsList) {
            WorkerData workerData = new WorkerData(parentAddress, settings);
            agents.get(workerData.getAddress().getAgentIndex() - 1).addWorker(workerData);
            workers.add(workerData);
        }
    }

    public synchronized void removeWorker(SimulatorAddress workerAddress) {
        for (WorkerData workerData : workers) {
            if (workerData.getAddress().equals(workerAddress)) {
                removeWorker(workerData);
                break;
            }
        }
    }

    public synchronized void removeWorker(WorkerData workerData) {
        agents.get(workerData.getAddress().getAgentIndex() - 1).removeWorker(workerData);
        workers.remove(workerData);
    }

    public int workerCount() {
        return workers.size();
    }

    public boolean hasClientWorkers() {
        for (WorkerData workerData : workers) {
            if (!workerData.isMemberWorker()) {
                return true;
            }
        }
        return false;
    }

    public List<WorkerData> getWorkers() {
        return unmodifiableList(workers);
    }

    public List<WorkerData> getWorkers(TargetType targetType, int targetCount) {
        if (targetCount <= 0) {
            return emptyList();
        }

        List<WorkerData> workerList = new ArrayList<WorkerData>();
        getWorkers(targetType, targetCount, workerList, true);
        return workerList;
    }

    public List<String> getWorkerAddresses(TargetType targetType, int targetCount) {
        if (targetCount <= 0) {
            return emptyList();
        }

        List<String> workerList = new ArrayList<String>();
        getWorkers(targetType, targetCount, workerList, false);
        return workerList;
    }

    @SuppressWarnings("unchecked")
    private void getWorkers(TargetType targetType, int targetCount, List workerList, boolean addWorkerData) {
        if (targetCount > workers.size()) {
            throw new IllegalArgumentException(format("Cannot return more Workers than registered (wanted: %d, registered: %d)",
                    targetCount, workers.size()));
        }

        int workersPerAgent = (int) ceil(targetCount / (double) agents.size());
        for (AgentData agentData : agents) {
            int count = 0;
            for (WorkerData workerData : agentData.getWorkers()) {
                if (!targetType.matches(workerData.isMemberWorker())) {
                    continue;
                }
                if (count++ < workersPerAgent && workerList.size() < targetCount) {
                    if (addWorkerData) {
                        workerList.add(workerData);
                    } else {
                        workerList.add(workerData.getAddress().toString());
                    }
                }
            }
        }

        if (workerList.size() < targetCount) {
            throw new IllegalStateException(format("Could not find enough Workers of type %s (wanted: %d, found: %d)",
                    targetType, targetCount, workerList.size()));
        }
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

    public synchronized void addTests(TestSuite testSuite) {
        for (TestCase testCase : testSuite.getTestCaseList()) {
            int addressIndex = testIndex.incrementAndGet();
            SimulatorAddress testAddress = new SimulatorAddress(AddressLevel.TEST, 0, 0, addressIndex);
            TestData testData = new TestData(addressIndex, testAddress, testCase);
            tests.put(testCase.getId(), testData);
        }
    }

    public void removeTests() {
        tests.clear();
    }

    public int testCount() {
        return tests.size();
    }

    public Collection<TestData> getTests() {
        return tests.values();
    }

    public TestData getTest(String testId) {
        return tests.get(testId);
    }
}
