package com.hazelcast.simulator.protocol.configuration;

import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.processors.OperationProcessor;

abstract class AbstractServerConfiguration implements ServerConfiguration {

    final OperationProcessor processor;
    final SimulatorAddress localAddress;

    private final int addressIndex;
    private final int port;

    AbstractServerConfiguration(OperationProcessor processor, SimulatorAddress localAddress, int port) {
        this.processor = processor;
        this.localAddress = localAddress;
        this.addressIndex = localAddress.getAddressIndex();
        this.port = port;
    }

    @Override
    public SimulatorAddress getLocalAddress() {
        return localAddress;
    }

    @Override
    public int getLocalPort() {
        return port;
    }

    @Override
    public String createFutureKey(long messageId) {
        return messageId + "_" + addressIndex;
    }
}
