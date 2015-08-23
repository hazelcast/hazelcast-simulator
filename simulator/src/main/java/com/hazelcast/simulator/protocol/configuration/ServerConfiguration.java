package com.hazelcast.simulator.protocol.configuration;

import com.hazelcast.simulator.protocol.core.MessageFuture;
import com.hazelcast.simulator.protocol.core.Response;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
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
     * Returns the {@link SimulatorAddress} of the local Simulator component.
     *
     * @return the local {@link SimulatorAddress}
     */
    SimulatorAddress getLocalAddress();

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
     * @param pipeline  the {@link ChannelPipeline} which should be configured
     * @param futureMap the map of {@link MessageFuture} for write methods to this client
     */
    void configurePipeline(ChannelPipeline pipeline, ConcurrentMap<String, MessageFuture<Response>> futureMap);

    /**
     * Created a map key for a {@link MessageFuture}.
     *
     * @param messageId the messageId of a {@link com.hazelcast.simulator.protocol.core.SimulatorMessage}
     * @return the key for the {@link MessageFuture} map
     */
    String createFutureKey(long messageId);
}
