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

import com.hazelcast.simulator.protocol.core.Response;
import com.hazelcast.simulator.protocol.core.ResponseFuture;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.operation.SimulatorOperation;

public interface Connector {

    /**
     * Submits a {@link SimulatorOperation} to a {@link SimulatorAddress}.
     *
     * The {@link SimulatorOperation} is put on a queue and a {@link ResponseFuture} is returned to wait for the result.
     * Does not support a destination {@link SimulatorAddress} with a wildcard.
     *
     * @param destination the {@link SimulatorAddress} of the destination
     * @param op   the {@link SimulatorOperation} to send
     * @return a {@link ResponseFuture} to wait for the result of the operation
     */
    ResponseFuture submit(SimulatorAddress destination, SimulatorOperation op);

    ResponseFuture submit(SimulatorAddress source, SimulatorAddress destination, SimulatorOperation op);

    /**
     * Writes a {@link SimulatorOperation} to a {@link SimulatorAddress}.
     *
     * Blocks until the {@link Response} is received.
     *
     * @param destination the {@link SimulatorAddress} of the destination
     * @param op   the {@link SimulatorOperation} to send
     * @return a {@link Response} with the result of the call
     */
    Response invoke(SimulatorAddress destination, SimulatorOperation op);

    /**
     * Writes a {@link SimulatorOperation} to a {@link SimulatorAddress}.
     *
     * Blocks until the {@link Response} is received.
     *
     * @param source      the {@link SimulatorAddress} of the source
     * @param destination the {@link SimulatorAddress} of the destination
     * @param op   the {@link SimulatorOperation} to send
     * @return a {@link Response} with the result of the call
     */
    Response invoke(SimulatorAddress source, SimulatorAddress destination, SimulatorOperation op);

    /**
     * Writes a {@link SimulatorOperation} to a {@link SimulatorAddress}.
     *
     * Does not support a destination {@link SimulatorAddress} with a wildcard.
     *
     * @param destination the {@link SimulatorAddress} of the destination
     * @param op   the {@link SimulatorOperation} to send
     * @return a {@link ResponseFuture} with returns the result of the call
     */
    ResponseFuture invokeAsync(SimulatorAddress destination, SimulatorOperation op);

    /**
     * Writes a {@link SimulatorOperation} to a {@link SimulatorAddress}.
     *
     * Does not support a destination {@link SimulatorAddress} with a wildcard.
     *
     * @param source      the {@link SimulatorAddress} of the source
     * @param destination the {@link SimulatorAddress} of the destination
     * @param op   the {@link SimulatorOperation} to send
     * @return a {@link ResponseFuture} with returns the result of the call
     */
    ResponseFuture invokeAsync(SimulatorAddress source, SimulatorAddress destination, SimulatorOperation op);
}
