package com.hazelcast.simulator.protocol.core;

/**
 * Defines the type for a {@link Response}, e.g. if it was successful or a specific error occurred.
 */
public enum ResponseType {

    SUCCESS(0),

    FAILURE_AGENT_NOT_FOUND(1),
    FAILURE_WORKER_NOT_FOUND(2),
    FAILURE_TEST_NOT_FOUND(3),

    FAILURE_RESPONSE_HAS_WILDCARD_DESTINATION(4);

    private final int ordinal;

    ResponseType(int ordinal) {
        this.ordinal = ordinal;
    }

    public static ResponseType fromInt(int intValue) {
        return ResponseType.values()[intValue];
    }

    public int toInt() {
        return ordinal;
    }
}
