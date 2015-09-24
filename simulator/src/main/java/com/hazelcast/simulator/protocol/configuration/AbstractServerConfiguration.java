package com.hazelcast.simulator.protocol.configuration;

import com.hazelcast.simulator.protocol.core.ResponseFuture;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.processors.OperationProcessor;

import java.util.concurrent.ConcurrentMap;

abstract class AbstractServerConfiguration implements ServerConfiguration {

    private final OperationProcessor processor;
    private final ConcurrentMap<String, ResponseFuture> futureMap;

    private final SimulatorAddress localAddress;
    private final int addressIndex;

    private final int port;

    AbstractServerConfiguration(OperationProcessor processor, ConcurrentMap<String, ResponseFuture> futureMap,
                                SimulatorAddress localAddress, int port) {
        this.processor = processor;
        this.futureMap = futureMap;

        this.localAddress = localAddress;
        this.addressIndex = localAddress.getAddressIndex();

        this.port = port;
    }

    @Override
    public void shutdown() {
        processor.shutdown();
    }

    @Override
    public SimulatorAddress getLocalAddress() {
        return localAddress;
    }

    @Override
    public int getLocalAddressIndex() {
        return addressIndex;
    }

    @Override
    public int getLocalPort() {
        return port;
    }

    @Override
    public OperationProcessor getProcessor() {
        return processor;
    }

    @Override
    public ConcurrentMap<String, ResponseFuture> getFutureMap() {
        return futureMap;
    }
}
