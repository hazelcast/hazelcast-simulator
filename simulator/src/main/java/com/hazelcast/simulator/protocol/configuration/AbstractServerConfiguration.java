package com.hazelcast.simulator.protocol.configuration;

import com.hazelcast.simulator.protocol.core.SimulatorAddress;

abstract class AbstractServerConfiguration implements ServerConfiguration {

    final SimulatorAddress localAddress;

    private final int addressIndex;
    private final int port;

    AbstractServerConfiguration(SimulatorAddress localAddress, int addressIndex, int port) {
        this.localAddress = localAddress;
        this.addressIndex = addressIndex;
        this.port = port;
    }

    @Override
    public int getAddressIndex() {
        return addressIndex;
    }

    @Override
    public int getPort() {
        return port;
    }
}
