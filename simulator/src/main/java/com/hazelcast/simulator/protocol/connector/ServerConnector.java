/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.simulator.protocol.connector;

import com.hazelcast.simulator.protocol.core.ResponseFuture;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;

import java.io.Closeable;
import java.util.concurrent.ConcurrentMap;

/**
 * Connector which listens for incoming Simulator component connections.
 */
public interface ServerConnector extends Connector, Closeable {

    int DEFAULT_SHUTDOWN_QUIET_PERIOD = 0;
    int DEFAULT_SHUTDOWN_TIMEOUT = 15;

    /**
     * Starts to listen on the incoming port.
     */
    void start();

    /**
     * Stops to listen on the incoming port.
     */
    void close();

    /**
     * Returns the {@link SimulatorAddress} of this Simulator component.
     *
     * @return the local {@link SimulatorAddress}
     */
    SimulatorAddress getAddress();

    /**
     * Returns the port on which the local Simulator component listens.
     *
     * @return the local port
     */
    int getPort();

    /**
     * Returns the map for {@link ResponseFuture} instances.
     *
     * @return the {@link ResponseFuture} map
     */
    ConcurrentMap<String, ResponseFuture> getFutureMap();
}
