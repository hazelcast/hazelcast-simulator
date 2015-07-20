package com.hazelcast.simulator.protocol.configuration;

import com.hazelcast.simulator.protocol.core.AddressLevel;
import io.netty.channel.ChannelPipeline;

/**
 * Configuration interface for a Simulator {@link com.hazelcast.simulator.protocol.connector.ServerConnector}.
 */
public interface BootstrapConfiguration {

    int DEFAULT_SHUTDOWN_QUIET_PERIOD = 0;
    int DEFAULT_SHUTDOWN_TIMEOUT = 15;

    AddressLevel getAddressLevel();

    int getAddressIndex();

    int getPort();

    void configurePipeline(ChannelPipeline pipeline);
}
