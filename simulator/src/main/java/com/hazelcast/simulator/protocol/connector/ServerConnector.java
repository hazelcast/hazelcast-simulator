/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.simulator.protocol.core.Response;
import com.hazelcast.simulator.protocol.core.ResponseFuture;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.operation.SimulatorOperation;

import java.util.concurrent.ConcurrentMap;

/**
 * Connector which listens for incoming Simulator component connections.
 */
public interface ServerConnector {

    int DEFAULT_SHUTDOWN_QUIET_PERIOD = 0;
    int DEFAULT_SHUTDOWN_TIMEOUT = 15;

    /**
     * Starts to listen on the incoming port.
     */
    void start();

    /**
     * Stops to listen on the incoming port.
     */
    void shutdown();

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

    /**
     * Submits a {@link SimulatorOperation} to a {@link SimulatorAddress}.
     *
     * The {@link SimulatorOperation} is put on a queue. The {@link Response} is not returned.
     *
     * @param destination the {@link SimulatorAddress} of the destination
     * @param operation   the {@link SimulatorOperation} to send
     * @return a {@link ResponseFuture} to wait for the result of the operation
     */
    ResponseFuture submit(SimulatorAddress destination, SimulatorOperation operation);

    /**
     * Writes a {@link SimulatorOperation} to a {@link SimulatorAddress} via the connected {@link ClientConnector}.
     *
     * Blocks until the {@link Response} is received.
     *
     * @param destination the {@link SimulatorAddress} of the destination
     * @param operation   the {@link SimulatorOperation} to send
     * @return a {@link Response} with the result of the call
     */
    Response write(SimulatorAddress destination, SimulatorOperation operation);

    /**
     * Writes a {@link SimulatorOperation} to a {@link SimulatorAddress} via the connected {@link ClientConnector}.
     *
     * Blocks until the {@link Response} is received.
     *
     * @param source      the {@link SimulatorAddress} of the source
     * @param destination the {@link SimulatorAddress} of the destination
     * @param operation   the {@link SimulatorOperation} to send
     * @return a {@link Response} with the result of the call
     */
    Response write(SimulatorAddress source, SimulatorAddress destination, SimulatorOperation operation);

    /**
     * Writes a {@link SimulatorOperation} to a {@link SimulatorAddress} via the connected {@link ClientConnector}.
     *
     * @param destination the {@link SimulatorAddress} of the destination
     * @param operation   the {@link SimulatorOperation} to send
     * @return a {@link ResponseFuture} with returns the result of the call
     */
    ResponseFuture writeAsync(SimulatorAddress destination, SimulatorOperation operation);

    /**
     * Writes a {@link SimulatorOperation} to a {@link SimulatorAddress} via the connected {@link ClientConnector}.
     *
     * @param source      the {@link SimulatorAddress} of the source
     * @param destination the {@link SimulatorAddress} of the destination
     * @param operation   the {@link SimulatorOperation} to send
     * @return a {@link ResponseFuture} with returns the result of the call
     */
    ResponseFuture writeAsync(SimulatorAddress source, SimulatorAddress destination, SimulatorOperation operation);
}
