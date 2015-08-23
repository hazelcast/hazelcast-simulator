package com.hazelcast.simulator.protocol.configuration;

import com.hazelcast.simulator.protocol.core.SimulatorAddress;

public abstract class AbstractClientConfiguration implements ClientConfiguration {

    final SimulatorAddress localAddress;
    final SimulatorAddress targetAddress;
    final int targetIndex;

    private final String host;
    private final int port;

    public AbstractClientConfiguration(SimulatorAddress localAddress, int targetIndex, String host, int port) {
        this.localAddress = localAddress;
        this.targetAddress = localAddress.getChild(targetIndex);
        this.targetIndex = targetIndex;
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
        return messageId + "_" + targetIndex;
    }
}
