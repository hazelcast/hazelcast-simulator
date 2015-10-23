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

import com.hazelcast.simulator.protocol.core.ResponseFuture;
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
     * Returns the address index of the remote Simulator component.
     *
     * @return the remote address index
     */
    int getRemoteIndex();

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
     * @param pipeline the {@link ChannelPipeline} which should be configured
     */
    void configurePipeline(ChannelPipeline pipeline);

    /**
     * Returns the map for {@link ResponseFuture} instances.
     *
     * @return the {@link ResponseFuture} map
     */
    ConcurrentMap<String, ResponseFuture> getFutureMap();
}
