package com.hazelcast.simulator.protocol.configuration;

import com.hazelcast.simulator.protocol.connector.ServerConnector;
import com.hazelcast.simulator.protocol.core.ResponseFuture;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.processors.OperationProcessor;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.group.ChannelGroup;

import java.util.concurrent.ConcurrentMap;

/**
 * Configuration interface for a Simulator {@link com.hazelcast.simulator.protocol.connector.ServerConnector}.
 */
public interface ServerConfiguration {

    int DEFAULT_SHUTDOWN_QUIET_PERIOD = 0;
    int DEFAULT_SHUTDOWN_TIMEOUT = 15;

    /**
     * Handles the shutdown of internal data structures.
     */
    void shutdown();

    /**
     * Returns the {@link SimulatorAddress} of the local Simulator component.
     *
     * @return the local {@link SimulatorAddress}
     */
    SimulatorAddress getLocalAddress();

    /**
     * Returns the address index of the local Simulator component.
     *
     * @return the local address index
     */
    int getLocalAddressIndex();

    /**
     * Returns the port on which the local Simulator component listens.
     *
     * @return the local port
     */
    int getLocalPort();

    /**
     * Returns the {@link ChannelGroup} of all connected client {@link io.netty.channel.Channel}.
     *
     * You can write to the {@link ChannelGroup} to send a message to all clients.
     *
     * @return the {@link ChannelGroup} of the connected clients
     */
    ChannelGroup getChannelGroup();

    /**
     * Configured the {@link ChannelPipeline} of the {@link com.hazelcast.simulator.protocol.connector.ServerConnector}.
     *
     * @param pipeline the {@link ChannelPipeline} which should be configured
     * @param connector the {@link ServerConnector} of the channel pipeline
     */
    void configurePipeline(ChannelPipeline pipeline, ServerConnector connector);

    /**
     * Returns the {@link OperationProcessor} of the local Simulator component.
     *
     * @return the {@link OperationProcessor}
     */
    OperationProcessor getProcessor();

    /**
     * Returns the map for {@link ResponseFuture} instances.
     *
     * @return the {@link ResponseFuture} map
     */
    ConcurrentMap<String, ResponseFuture> getFutureMap();
}
