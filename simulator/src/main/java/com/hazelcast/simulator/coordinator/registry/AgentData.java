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
import com.hazelcast.simulator.utils.TagUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.simulator.common.SimulatorProperties.DEFAULT_BROKER_PORT;
import static com.hazelcast.simulator.common.SimulatorProperties.DEFAULT_SSH_PORT;
import static com.hazelcast.simulator.common.SimulatorProperties.PUBLIC_BROKER_PORT_TAG;
import static com.hazelcast.simulator.common.SimulatorProperties.PUBLIC_SSH_PORT_TAG;
import static com.hazelcast.simulator.utils.FormatUtils.formatIpAddress;
import static com.hazelcast.simulator.utils.FormatUtils.join;
import static com.hazelcast.simulator.utils.Preconditions.checkNotNull;
import static java.util.stream.Collectors.toList;

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
    private final Collection<WorkerData> workers = new ArrayList<>();
    private final AtomicInteger currentWorkerIndex = new AtomicInteger();
    private final int addressIndex;
    private final SimulatorAddress address;
    private final String publicAddress;
    private final String privateAddress;
    private final Map<String, String> tags;
    private final int publicSshPort;
    private final int publicBrokerPort;

    public AgentData(int addressIndex, String publicAddress, String privateAddress) {
        this(addressIndex, publicAddress, privateAddress, new HashMap<>());
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
        this.publicSshPort = TagUtils.portFromTagsOrDefault(tags, PUBLIC_SSH_PORT_TAG, DEFAULT_SSH_PORT);
        this.publicBrokerPort = TagUtils.portFromTagsOrDefault(tags, PUBLIC_BROKER_PORT_TAG, DEFAULT_BROKER_PORT);
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

    public IpAndPort getPublicSshAddress() {
        return new IpAndPort(publicAddress, publicSshPort);
    }

    public IpAndPort getPublicBrokerAddress() {
        return new IpAndPort(publicAddress, publicBrokerPort);
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
        Set<String> result = new HashSet<>();
        for (WorkerData worker : workers) {
            result.add(worker.getParameters().get("VERSION_SPEC"));
        }
        return result;
    }

    public String formatIpAddresses() {
        String publicIp = getPublicSshAddress().getIp();
        int sshPort = getPublicSshAddress().getPort();
        String formattedPublicIp = formatIpAddress(publicIp);
        if (sshPort != DEFAULT_SSH_PORT) {
            formattedPublicIp += ":" + sshPort;
        }

        String formattedPrivateIp = formatIpAddress(getPrivateAddress());
        if (formattedPublicIp.equals(formattedPrivateIp)) {
            return formattedPublicIp;
        }
        return formattedPublicIp + " " + formattedPrivateIp;
    }

    @Override
    public String toString() {
        return "AgentData{address=" + address + '}';
    }

    public static List<IpAndPort> publicSshAddresses(List<AgentData> agents) {
        return agents.stream().map(AgentData::getPublicSshAddress).collect(toList());
    }

    public static String publicAddressesString(List<AgentData> agents) {
        return join(publicSshAddresses(agents), ",");
    }

    public static String publicAddressesString(Registry registry) {
        return publicAddressesString(registry.getAgents());
    }
}
