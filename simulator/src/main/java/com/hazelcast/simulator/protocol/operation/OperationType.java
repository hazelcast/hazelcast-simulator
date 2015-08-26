package com.hazelcast.simulator.protocol.operation;

/**
 * Defines the operation type for a {@link com.hazelcast.simulator.protocol.core.SimulatorMessage}.
 */
public enum OperationType {

    INTEGRATION_TEST(IntegrationTestOperation.class, 0),

    CREATE_AGENT(IntegrationTestOperation.class, 1),
    CREATE_WORKER(IntegrationTestOperation.class, 2),
    CREATE_TEST(CreateTestOperation.class, 3);

    private final Class<? extends SimulatorOperation> classType;
    private final int classId;

    OperationType(Class<? extends SimulatorOperation> classType, int classId) {
        this.classType = classType;
        this.classId = classId;
    }

    public static OperationType fromInt(int classId) {
        try {
            return OperationType.values()[classId];
        } catch (ArrayIndexOutOfBoundsException e) {
            throw new IllegalArgumentException("Unknown message type: " + classId, e);
        }
    }

    public int toInt() {
        return classId;
    }

    public Class<? extends SimulatorOperation> getClassType() {
        return classType;
    }
}
