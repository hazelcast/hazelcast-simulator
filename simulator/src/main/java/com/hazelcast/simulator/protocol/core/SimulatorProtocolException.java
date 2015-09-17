package com.hazelcast.simulator.protocol.core;

/**
 * Exception thrown when an internal error occurs in the Simulator Protocol.
 */
public class SimulatorProtocolException extends RuntimeException {

    public SimulatorProtocolException(String message) {
        super(message);
    }

    public SimulatorProtocolException(String message, Throwable cause) {
        super(message, cause);
    }
}
