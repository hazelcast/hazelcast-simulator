package com.hazelcast.simulator.protocol.configuration;

import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.processors.OperationProcessor;

public abstract class AbstractClientConfiguration implements ClientConfiguration {

    final OperationProcessor processor;
    final SimulatorAddress localAddress;
    final SimulatorAddress remoteAddress;

    private final int remoteIndex;
    private final String remoteHost;
    private final int remotePort;

    public AbstractClientConfiguration(OperationProcessor processor, SimulatorAddress localAddress, int remoteIndex,
                                       String remoteHost, int remotePort) {
        this.processor = processor;
        this.localAddress = localAddress;
        this.remoteAddress = localAddress.getChild(remoteIndex);
        this.remoteIndex = remoteIndex;
        this.remoteHost = remoteHost;
        this.remotePort = remotePort;
    }

    @Override
    public SimulatorAddress getLocalAddress() {
        return localAddress;
    }

    @Override
    public SimulatorAddress getRemoteAddress() {
        return remoteAddress;
    }

    @Override
    public String getRemoteHost() {
        return remoteHost;
    }

    @Override
    public int getRemotePort() {
        return remotePort;
    }

    @Override
    public String createFutureKey(long messageId) {
        return messageId + "_" + remoteIndex;
    }
}
