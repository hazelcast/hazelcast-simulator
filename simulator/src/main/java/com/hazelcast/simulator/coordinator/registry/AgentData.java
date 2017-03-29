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

import com.hazelcast.simulator.protocol.core.SimulatorAddress;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.simulator.utils.FormatUtils.formatIpAddress;
import static com.hazelcast.simulator.utils.FormatUtils.join;
import static com.hazelcast.simulator.utils.Preconditions.checkNotNull;

/**
 * Contains the metadata of a Simulator Agent.
 * <p>
 * Part of the metadata is the {@link SimulatorAddress} which is used by the Simulator Communication Protocol.
 * <p>
 * The metadata also contains the IP addresses to connect to the Agent via network. We have a public and private address to deal
 * with cloud environments.
 * <p>
 * The public address is used by the 'outside' systems like Coordinator to talk to the agents. The private address is used for
 * Hazelcast instances to communicate with each other. They are the same if there is no need for an address separation, e.g. in
 * a static setup.
 */
public class AgentData {

    public enum AgentWorkerMode {
        MEMBERS_ONLY,
        CLIENTS_ONLY,
        MIXED
    }

    private AgentWorkerMode agentWorkerMode = AgentWorkerMode.MIXED;
    private final Collection<WorkerData> workers = new ArrayList<WorkerData>();
    private final AtomicInteger currentWorkerIndex = new AtomicInteger();
    private final int addressIndex;
    private final SimulatorAddress address;
    private final String publicAddress;
    private final String privateAddress;
    private final Map<String, String> tags;

    public AgentData(int addressIndex, String publicAddress, String privateAddress) {
        this(addressIndex, publicAddress, privateAddress, new HashMap<String, String>());
    }

    public AgentData(int addressIndex, String publicAddress, String privateAddress, Map<String, String> tags) {
        if (addressIndex <= 0) {
            throw new IllegalArgumentException("addressIndex must be a positive number");
        }
        this.addressIndex = addressIndex;
        this.address = SimulatorAddress.agentAddress(addressIndex);
        this.publicAddress = checkNotNull(publicAddress, "publicAddress can't be null");
        this.privateAddress = checkNotNull(privateAddress, "privateAddress can't be null");
        this.tags = checkNotNull(tags, "tags can't be null");
    }

    public Map<String, String> getTags() {
        return tags;
    }

    public void setAgentWorkerMode(AgentWorkerMode agentWorkerMode) {
        this.agentWorkerMode = checkNotNull(agentWorkerMode, "agentWorkerMode can't be null");
    }

    public AgentWorkerMode getAgentWorkerMode() {
        return agentWorkerMode;
    }

    public int getAddressIndex() {
        return addressIndex;
    }

    public SimulatorAddress getAddress() {
        return address;
    }

    public String getPublicAddress() {
        return publicAddress;
    }

    public String getPrivateAddress() {
        return privateAddress;
    }

    public int getNextWorkerIndex() {
        return currentWorkerIndex.incrementAndGet();
    }

    public int getCurrentWorkerIndex() {
        return currentWorkerIndex.incrementAndGet();
    }

    /**
     * Used for tests to set a correct value for the next Worker index.
     *
     * @param workerIndex the index of an added Worker
     */
    void updateWorkerIndex(int workerIndex) {
        if (workerIndex > currentWorkerIndex.get()) {
            currentWorkerIndex.set(workerIndex);
        }
    }

    public Collection<WorkerData> getWorkers() {
        return workers;
    }

    void addWorker(WorkerData worker) {
        workers.add(worker);
    }

    void removeWorker(WorkerData worker) {
        workers.remove(worker);
    }

    public int getCount(String workerType) {
        int result = 0;
        for (WorkerData worker : workers) {
            if (worker.getParameters().getWorkerType().equals(workerType)) {
                result++;
            }
        }
        return result;
    }

    public Set<String> getVersionSpecs() {
        Set<String> result = new HashSet<String>();
        for (WorkerData worker : workers) {
            result.add(worker.getParameters().get("VERSION_SPEC"));
        }
        return result;
    }

    public String formatIpAddresses() {
        String publicIp = formatIpAddress(getPublicAddress());
        String privateIp = formatIpAddress(getPrivateAddress());
        if (publicIp.equals(privateIp)) {
            return publicIp;
        }
        return publicIp + " " + privateIp;
    }

    @Override
    public String toString() {
        return "AgentData{address=" + address + '}';
    }

    public static List<String> publicAddresses(List<AgentData> agents) {
        List<String> result = new ArrayList<String>();
        for (AgentData agent : agents) {
            result.add(agent.getPublicAddress());
        }
        return result;
    }

    public static String publicAddressesString(List<AgentData> agents) {
        return join(publicAddresses(agents), ",");
    }

    public static String publicAddressesString(Registry registry) {
        return publicAddressesString(registry.getAgents());
    }
}
