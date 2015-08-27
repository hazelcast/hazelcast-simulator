package com.hazelcast.simulator.protocol.connector;

import com.hazelcast.simulator.protocol.core.Response;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.core.SimulatorMessage;

/**
 * Connector which listens for incoming Simulator component connections.
 */
public interface ServerConnector {

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
     * Writes a {@link SimulatorMessage} to the {@link ClientConnector} of connected Simulator components.
     *
     * @param message the {@link SimulatorMessage} to send.
     * @return a {@link Response} with the result of the call
     * @throws Exception if the {@link SimulatorMessage} could not be send
     */
    Response write(SimulatorMessage message) throws Exception;
}
