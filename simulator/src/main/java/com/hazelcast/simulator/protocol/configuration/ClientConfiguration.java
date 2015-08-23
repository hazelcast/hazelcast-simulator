package com.hazelcast.simulator.protocol.configuration;

import com.hazelcast.simulator.protocol.core.MessageFuture;
import com.hazelcast.simulator.protocol.core.Response;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import io.netty.channel.ChannelPipeline;

import java.util.concurrent.ConcurrentMap;

/**
 * Configuration interface for a Simulator {@link com.hazelcast.simulator.protocol.connector.ClientConnector}.
 */
public interface ClientConfiguration {

    /**
     * Returns the {@link SimulatorAddress} of the local Simulator component.
     *
     * @return the local {@link SimulatorAddress}
     */
    SimulatorAddress getLocalAddress();

    /**
     * Returns the {@link SimulatorAddress} of the remote Simulator component.
     *
     * @return the remote {@link SimulatorAddress}
     */
    SimulatorAddress getRemoteAddress();

    /**
     * Returns the host of the remote Simulator component.
     *
     * @return the remote host
     */
    String getRemoteHost();

    /**
     * Returns the port of the remote Simulator component.
     *
     * @return the remote port
     */
    int getRemotePort();

    /**
     * Configured the {@link ChannelPipeline} of the {@link com.hazelcast.simulator.protocol.connector.ClientConnector}.
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
