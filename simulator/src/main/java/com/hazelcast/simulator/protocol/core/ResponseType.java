package com.hazelcast.simulator.protocol.core;

/**
 * Defines the type for a {@link Response}, e.g. if it was successful or a specific error occurred.
 */
@SuppressWarnings("checkstyle:magicnumber")
public enum ResponseType {

    SUCCESS(0),

    FAILURE_AGENT_NOT_FOUND(1),
    FAILURE_WORKER_NOT_FOUND(2),
    FAILURE_TEST_NOT_FOUND(3);

    private final int ordinal;

    ResponseType(int ordinal) {
        this.ordinal = ordinal;
    }

    public static ResponseType fromInt(int intValue) {
        switch (intValue) {
            case 0:
                return SUCCESS;
            case 1:
                return FAILURE_AGENT_NOT_FOUND;
            case 2:
                return FAILURE_WORKER_NOT_FOUND;
            case 3:
                return FAILURE_TEST_NOT_FOUND;
            default:
                throw new IllegalArgumentException("Unknown responseType: " + intValue);
        }
    }

    public int toInt() {
        return ordinal;
    }
}
