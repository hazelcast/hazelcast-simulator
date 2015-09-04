package com.hazelcast.simulator.protocol.registry;

import com.hazelcast.simulator.protocol.core.AddressLevel;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;

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
}
