package com.hazelcast.simulator.protocol.configuration;

import com.hazelcast.simulator.protocol.core.ResponseFuture;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.processors.OperationProcessor;

import java.util.concurrent.ConcurrentMap;

abstract class AbstractClientConfiguration implements ClientConfiguration {

    final OperationProcessor processor;
    final ConcurrentMap<String, ResponseFuture> futureMap;

    final SimulatorAddress localAddress;
    final SimulatorAddress remoteAddress;

    private final int remoteIndex;
    private final String remoteHost;
    private final int remotePort;

    AbstractClientConfiguration(OperationProcessor processor, ConcurrentMap<String, ResponseFuture> futureMap,
                                       SimulatorAddress localAddress, int remoteIndex, String remoteHost, int remotePort) {
        this.processor = processor;
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

    @Override
    public String createFutureKey(long messageId) {
        return messageId + "_" + remoteIndex;
    }
}
