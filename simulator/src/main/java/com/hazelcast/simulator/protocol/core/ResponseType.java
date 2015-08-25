package com.hazelcast.simulator.protocol.core;

/**
 * Defines the type for a {@link Response}, e.g. if it was successful or a specific error occurred.
 */
public enum ResponseType {

    /**
     * Is returned when the {@link SimulatorMessage} was correctly processed.
     */
    SUCCESS(0),

    /**
     * Is returned when the addressed Agent was not found by the Coordinator.
     */
    FAILURE_AGENT_NOT_FOUND(1),

    /**
     * Is returned when the addressed Worker was not found by an Agent component.
     */
    FAILURE_WORKER_NOT_FOUND(2),

    /**
     * Is returned when the addressed Test was not found by a Worker component.
     */
    FAILURE_TEST_NOT_FOUND(3),

    /**
     * Is returned when an implementation of {@link com.hazelcast.simulator.protocol.processors.OperationProcessor}
     * does not implement the transmitted {@link com.hazelcast.simulator.protocol.operation.SimulatorOperation}.
     */
    UNSUPPORTED_OPERATION_ON_THIS_PROCESSOR(4);

    private final int ordinal;

    ResponseType(int ordinal) {
        this.ordinal = ordinal;
    }

    public static ResponseType fromInt(int intValue) {
        try {
            return ResponseType.values()[intValue];
        } catch (ArrayIndexOutOfBoundsException e) {
            throw new IllegalArgumentException("Unknown response type: " + intValue, e);
        }
    }

    public int toInt() {
        return ordinal;
    }
}
