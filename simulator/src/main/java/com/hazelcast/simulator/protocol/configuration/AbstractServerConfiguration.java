package com.hazelcast.simulator.protocol.configuration;

import com.hazelcast.simulator.protocol.core.ResponseFuture;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.processors.OperationProcessor;
import org.apache.log4j.Logger;

import java.util.concurrent.ConcurrentMap;

import static java.lang.String.format;

@SuppressWarnings("checkstyle:visibilitymodifier")
abstract class AbstractServerConfiguration implements ServerConfiguration {

    private static final Logger LOGGER = Logger.getLogger(AbstractServerConfiguration.class);

    final OperationProcessor processor;
    final ConcurrentMap<String, ResponseFuture> futureMap;

    final SimulatorAddress localAddress;
    final int addressIndex;

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
    public SimulatorAddress getLocalAddress() {
        return localAddress;
    }

    @Override
    public int getLocalPort() {
        return port;
    }

    @Override
    public ConcurrentMap<String, ResponseFuture> getFutureMap() {
        return futureMap;
    }

    @Override
    public String createFutureKey(long messageId) {
        String futureKey = messageId + "_" + addressIndex;
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("[%d] Created futureKey %s on server %s", messageId, futureKey, localAddress));
        }
        return futureKey;
    }
}
