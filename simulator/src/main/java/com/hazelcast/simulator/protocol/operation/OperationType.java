package com.hazelcast.simulator.protocol.operation;

public enum OperationType {

    INTEGRATION_TEST_OPERATION(IntegrationTestOperation.class, 0);

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
            throw new IllegalArgumentException("Unknown message type: " + classId);
        }
    }

    public int toInt() {
        return classId;
    }

    public Class<? extends SimulatorOperation> getClassType() {
        return classType;
    }
}
