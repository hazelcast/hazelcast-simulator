package com.hazelcast.simulator.protocol.configuration;

import com.hazelcast.simulator.protocol.core.ResponseFuture;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;

import java.util.concurrent.ConcurrentMap;

abstract class AbstractServerConfiguration implements ServerConfiguration {

    private final ConcurrentMap<String, ResponseFuture> futureMap;

    private final SimulatorAddress localAddress;
    private final int addressIndex;

    private final int port;

    AbstractServerConfiguration(ConcurrentMap<String, ResponseFuture> futureMap, SimulatorAddress localAddress, int port) {
        this.futureMap = futureMap;

        this.localAddress = localAddress;
        this.addressIndex = localAddress.getAddressIndex();

        this.port = port;
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
    public ConcurrentMap<String, ResponseFuture> getFutureMap() {
        return futureMap;
    }
}
