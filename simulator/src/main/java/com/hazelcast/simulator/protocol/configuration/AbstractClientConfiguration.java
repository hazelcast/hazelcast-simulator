package com.hazelcast.simulator.protocol.configuration;

import com.hazelcast.simulator.protocol.core.ResponseFuture;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;

import java.util.concurrent.ConcurrentMap;

abstract class AbstractClientConfiguration implements ClientConfiguration {

    private final ConcurrentMap<String, ResponseFuture> futureMap;

    private final SimulatorAddress localAddress;
    private final SimulatorAddress remoteAddress;

    private final int remoteIndex;
    private final String remoteHost;
    private final int remotePort;

    AbstractClientConfiguration(ConcurrentMap<String, ResponseFuture> futureMap, SimulatorAddress localAddress,
                                int remoteIndex, String remoteHost, int remotePort) {
        this.futureMap = futureMap;

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
    public int getRemoteIndex() {
        return remoteIndex;
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
    public ConcurrentMap<String, ResponseFuture> getFutureMap() {
        return futureMap;
    }
}
