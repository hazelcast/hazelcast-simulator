/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
