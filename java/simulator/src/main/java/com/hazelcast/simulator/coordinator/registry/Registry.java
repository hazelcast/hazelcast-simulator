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
package com.hazelcast.simulator.coordinator.registry;

import com.hazelcast.simulator.agent.workerprocess.WorkerParameters;
import com.hazelcast.simulator.common.TestCase;
import com.hazelcast.simulator.coordinator.TargetType;
import com.hazelcast.simulator.coordinator.TestSuite;
import com.hazelcast.simulator.coordinator.registry.AgentData.AgentWorkerMode;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.utils.BashCommand;
import com.hazelcast.simulator.utils.CommandLineExitException;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.simulator.coordinator.registry.AgentData.AgentWorkerMode.CLIENTS_ONLY;
import static com.hazelcast.simulator.coordinator.registry.AgentData.AgentWorkerMode.MEMBERS_ONLY;
import static com.hazelcast.simulator.coordinator.registry.AgentData.AgentWorkerMode.MIXED;
import static com.hazelcast.simulator.utils.FileUtils.locatePythonFile;
import static com.hazelcast.simulator.utils.FormatUtils.HORIZONTAL_RULER;
import static com.hazelcast.simulator.utils.FormatUtils.formatLong;
import static java.lang.Math.ceil;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Collections.synchronizedList;
import static java.util.Collections.unmodifiableList;

/**
 * Keeps track of all Simulator components which are running.
 */
@SuppressWarnings("checkstyle:methodcount")
public class Registry {
    private final AtomicInteger agentIndex = new AtomicInteger();
    private final AtomicInteger testIndexGenerator = new AtomicInteger();
    // a map with all id's and its count. This is used to make sure each id is unique.
    private final Map<String, AtomicLong> ids = new HashMap<>();
    private final List<AgentData> agents = synchronizedList(new ArrayList<>());
    private final List<WorkerData> workers = synchronizedList(new ArrayList<>());
    private final ConcurrentMap<String, TestData> tests = new ConcurrentHashMap<>();

    public void Registry() {
    }

    public AgentData addAgent(String publicAddress, String privateAddress) {
        return addAgent(publicAddress, privateAddress, new HashMap<>());
    }

    public AgentData addAgent(String publicAddress, String privateAddress, Map<String, String> tags) {
        AgentData agent = new AgentData(agentIndex.incrementAndGet(), publicAddress, privateAddress, tags);
        agents.add(agent);
        return agent;
    }

    public AgentData getAgent(String publicIp) {
        for (AgentData agentData : agents) {
            if (agentData.getPublicAddress().equals(publicIp)) {
                return agentData;
            }
        }
        return null;
    }

    public void assignDedicatedMemberMachines(int dedicatedMemberMachineCount) {
        if (dedicatedMemberMachineCount < 0) {
            throw new CommandLineExitException("dedicatedMemberMachines can't be smaller than 0");
        }

        if (dedicatedMemberMachineCount > agents.size()) {
            throw new CommandLineExitException("dedicatedMemberMachines can't be larger than the number of agents.");
        }

        if (dedicatedMemberMachineCount > 0) {
            assignAgentWorkerMode(0, dedicatedMemberMachineCount, MEMBERS_ONLY);
            assignAgentWorkerMode(dedicatedMemberMachineCount, agentCount(), CLIENTS_ONLY);
        }
    }

    private void assignAgentWorkerMode(int startIndex, int endIndex, AgentWorkerMode agentWorkerMode) {
        for (int i = startIndex; i < endIndex; i++) {
            AgentData agent = agents.get(i);
            agent.setAgentWorkerMode(agentWorkerMode);
        }
    }

    public void removeAgent(AgentData agent) {
        agents.remove(agent);
    }

    public int agentCount() {
        return agents.size();
    }

    public List<AgentData> getAgents() {
        return unmodifiableList(new LinkedList<>(agents));
    }

    public AgentData getAgent(SimulatorAddress simulatorAddress) {
        for (AgentData agent : agents) {
            if (agent.getAddress().equals(simulatorAddress)) {
                return agent;
            }
        }

        return null;
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

    public WorkerData getWorker(SimulatorAddress workerAddress) {
        for (WorkerData worker : workers) {
            if (worker.getAddress().equals(workerAddress)) {
                return worker;
            }
        }
        return null;
    }

    public List<WorkerData> addWorkers(List<WorkerParameters> settingsList) {
        return addWorkers(settingsList, new HashMap<>());
    }

    public synchronized List<WorkerData> addWorkers(
            List<WorkerParameters> workerParametersList,
            Map<String, String> tags) {
        List<WorkerData> result = new ArrayList<>(workerParametersList.size());
        for (WorkerParameters workerParameters : workerParametersList) {
            WorkerData worker = new WorkerData(workerParameters, tags);

            AgentData agent = agents.get(worker.getAddress().getAgentIndex() - 1);
            agent.addWorker(worker);
            agent.updateWorkerIndex(worker.getAddress().getAddressIndex());

            workers.add(worker);
            result.add(worker);
        }
        return result;
    }

    public synchronized void removeWorker(SimulatorAddress workerAddress) {
        for (WorkerData worker : workers) {
            if (worker.getAddress().equals(workerAddress)) {
                removeWorker(worker);
                break;
            }
        }
    }

    public synchronized WorkerData findWorker(SimulatorAddress workerAddress) {
        for (WorkerData worker : workers) {
            if (worker.getAddress().equals(workerAddress)) {
                return worker;
            }
        }

        return null;
    }

    public synchronized void removeWorker(WorkerData worker) {
        agents.get(worker.getAddress().getAgentIndex() - 1).removeWorker(worker);
        workers.remove(worker);
    }

    public int workerCount() {
        return workers.size();
    }

    public boolean hasClientWorkers() {
        for (WorkerData worker : workers) {
            if (!worker.isMemberWorker()) {
                return true;
            }
        }
        return false;
    }

    public List<WorkerData> getWorkers() {
        return new ArrayList<>(workers);
    }

    public List<WorkerData> getWorkers(TargetType targetType, int targetCount) {
        if (targetCount <= 0) {
            return emptyList();
        }

        List<WorkerData> workerList = new ArrayList<>();
        getWorkers(targetType, targetCount, workerList, true);
        return workerList;
    }

    public List<String> getWorkerAddresses(TargetType targetType, int targetCount) {
        if (targetCount <= 0) {
            return emptyList();
        }

        List<String> workerList = new ArrayList<>();
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
        for (AgentData agent : agents) {
            int count = 0;
            for (WorkerData worker : agent.getWorkers()) {
                if (!targetType.matches(worker.isMemberWorker())) {
                    continue;
                }
                if (count++ < workersPerAgent && workerList.size() < targetCount) {
                    if (addWorkerData) {
                        workerList.add(worker);
                    } else {
                        workerList.add(worker.getAddress().toString());
                    }
                }
            }
        }

        if (workerList.size() < targetCount) {
            throw new IllegalStateException(format("Could not find enough Workers of type %s (wanted: %d, found: %d)",
                    targetType, targetCount, workerList.size()));
        }
    }

    public String printLayout() {
        StringBuilder sb = new StringBuilder();

        sb.append(HORIZONTAL_RULER).append('\n');
        sb.append("Cluster layout").append('\n');
        sb.append(HORIZONTAL_RULER).append('\n');

        for (AgentData agent : agents) {

            Set<String> agentVersionSpecs = agent.getVersionSpecs();
            int agentMemberWorkerCount = agent.getCount("member");
            int agentClientWorkerCount = agent.getWorkers().size() - agentMemberWorkerCount;
            int totalWorkerCount = agentMemberWorkerCount + agentClientWorkerCount;

            String message = "    Agent %s (%s) members: %s, clients: %s";
            if (totalWorkerCount > 0) {
                message += ", version specs: %s";
            } else {
                message += " (no workers)";
            }

            sb.append(format(message,
                    agent.formatIpAddresses(),
                    agent.getAddress(),
                    formatLong(agentMemberWorkerCount, 2),
                    formatLong(agentClientWorkerCount, 2),
                    agentVersionSpecs)).append('\n');

            for (WorkerData worker : agent.getWorkers()) {
                WorkerParameters parameters = worker.getParameters();
                sb.append("        Worker ")
                        .append(worker.getAddress())
                        .append(" ").append(parameters.getWorkerType())
                        .append(" [").append(parameters.get("VERSION_SPEC")).append("]")
                        .append('\n');
            }
        }

        List<TestData> tests = new ArrayList<>(this.tests.values());
        sb.append(format("    Tests %s", tests.size())).append('\n');
        for (TestData test : tests) {
            sb.append("        ")
                    .append(test.getTestCase().getId())
                    .append(" ")
                    .append(test.getStatusString())
                    .append('\n');
        }

        return sb.toString();
    }

    public synchronized List<TestData> addTests(TestSuite testSuite) {
        List<TestData> result = new ArrayList<>(testSuite.size());
        for (TestCase testCase : testSuite.getTestCaseList()) {
            String id = testCase.getId();
            AtomicLong count = ids.get(id);
            if (count == null) {
                ids.put(id, new AtomicLong(1));
            } else {
                id = id + "__" + count.getAndIncrement();
            }
            int testIndex = testIndexGenerator.incrementAndGet();
            testCase.setId(id);

            TestData test = new TestData(testIndex, testCase, testSuite);
            result.add(test);
            tests.put(id, test);
        }
        return result;
    }

    public int testCount() {
        return tests.size();
    }

    public Collection<TestData> getTests() {
        return new ArrayList<>(tests.values());
    }

    public TestData getTest(String testId) {
        return tests.get(testId);
    }

    public static Registry loadInventoryYaml(File file, String loadGeneratorHosts, String nodeHosts) {
        if (nodeHosts == null) {
            throw new NullPointerException();
        }

        Registry registry = new Registry();
        List<AgentData> nodes = loadAgents(registry, nodeHosts);
        List<AgentData> loadGenerators = loadAgents(registry, loadGeneratorHosts);

        for (AgentData agentData : registry.getAgents()) {
            boolean isNode = nodes.contains(agentData);
            boolean isLoadGenerator = loadGenerators.contains(agentData);

            if (isNode && !isLoadGenerator) {
                agentData.setAgentWorkerMode(MEMBERS_ONLY);
            } else if (!isNode && isLoadGenerator) {
                agentData.setAgentWorkerMode(CLIENTS_ONLY);
            } else {
                agentData.setAgentWorkerMode(MIXED);
            }
        }

        return registry;
    }

    private static List<AgentData> loadAgents(Registry registry, String nodeHosts) {
        List<AgentData> agents = new ArrayList<>();
        if (nodeHosts == null) {
            return agents;
        }

        String out = new BashCommand(locatePythonFile("load_hosts.py"))
                .addParams(nodeHosts)
                .execute();

        Yaml yaml = new Yaml();
        List<Map> hosts = yaml.load(out);

        for (Map<String, Object> host : hosts) {
            String publicIp = (String) host.get("public_ip");
            AgentData agentData = registry.getAgent(publicIp);
            if (agentData == null) {
                String groupName = (String) host.get("groupname");
                String privateIp = (String) host.get("private_ip");
                if (privateIp == null) {
                    privateIp = publicIp;
                }

                Map<String, String> tags = new HashMap<>();
                tags.put("group", groupName);
                for (String key : host.keySet()) {
                    if (key.equals("public_ip") || key.equals("private_ip")) {
                        continue;
                    }
                    tags.put(key, "" + host.get(key));
                }
                agentData = registry.addAgent(publicIp, privateIp, tags);
                agentData.setSshOptions((String) host.get("ssh_options"));
                agentData.setSshUser((String) host.get("ssh_user"));
            }
            agents.add(agentData);
        }
        return agents;
    }

}
