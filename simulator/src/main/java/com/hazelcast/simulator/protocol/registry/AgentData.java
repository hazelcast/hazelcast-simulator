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

import com.hazelcast.simulator.protocol.core.AddressLevel;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;

import java.util.ArrayList;
import java.util.Collection;

/**
 * Contains the metadata of a Simulator Agent.
 *
 * Part of the metadata is the {@link SimulatorAddress} which is used by the Simulator Communication Protocol.
 *
 * The metadata also contains the IP addresses to connect to the Agent via network. We have a public and private address to deal
 * with cloud environments.
 *
 * The public address is used by the 'outside' systems like Coordinator to talk to the agents. The private address is used for
 * Hazelcast instances to communicate with each other. They are the same if there is no need for an address separation, e.g. in
 * a static setup.
 */
public class AgentData {

    private final Collection<WorkerData> workers = new ArrayList<WorkerData>();

    private final int addressIndex;
    private final SimulatorAddress address;

    private final String publicAddress;
    private final String privateAddress;

    public AgentData(int addressIndex, String publicAddress, String privateAddress) {
        if (addressIndex <= 0) {
            throw new IllegalArgumentException("addressIndex must be a positive number");
        }
        if (publicAddress == null) {
            throw new NullPointerException("publicAddress can't be null");
        }
        if (privateAddress == null) {
            throw new NullPointerException("privateAddress can't be null");
        }

        this.addressIndex = addressIndex;
        this.address = new SimulatorAddress(AddressLevel.AGENT, addressIndex, 0, 0);
        this.publicAddress = publicAddress;
        this.privateAddress = privateAddress;
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

    void addWorker(WorkerData workerData) {
        workers.add(workerData);
    }

    void removeWorker(WorkerData workerData) {
        workers.remove(workerData);
    }

    Collection<WorkerData> getWorkers() {
        return workers;
    }
}
