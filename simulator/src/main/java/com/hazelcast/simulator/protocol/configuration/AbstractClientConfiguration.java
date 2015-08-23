package com.hazelcast.simulator.protocol.configuration;

import com.hazelcast.simulator.protocol.core.AddressLevel;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;

public abstract class AbstractClientConfiguration implements ClientConfiguration {

    final SimulatorAddress localAddress;
    final AddressLevel addressLevel;
    final int addressIndex;

    private final String host;
    private final int port;

    public AbstractClientConfiguration(SimulatorAddress localAddress, AddressLevel addressLevel, int addressIndex,
                                       String host, int port) {
        this.localAddress = localAddress;
        this.addressLevel = addressLevel;
        this.addressIndex = addressIndex;
        this.host = host;
        this.port = port;
    }

    @Override
    public String getHost() {
        return host;
    }

    @Override
    public int getPort() {
        return port;
    }

    @Override
    public String createFutureKey(long messageId) {
        return messageId + "_" + addressIndex;
    }
}
