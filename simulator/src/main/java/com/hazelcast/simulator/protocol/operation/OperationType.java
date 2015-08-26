package com.hazelcast.simulator.protocol.operation;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static java.lang.String.format;

/**
 * Defines the operation type for a {@link com.hazelcast.simulator.protocol.core.SimulatorMessage}.
 */
public enum OperationType {

    INTEGRATION_TEST(IntegrationTestOperation.class, 0),

    //CREATE_AGENT(IntegrationTestOperation.class, 1),
    //CREATE_WORKER(IntegrationTestOperation.class, 2),
    CREATE_TEST(CreateTestOperation.class, 3),

    IS_PHASE_COMPLETED(IsPhaseCompletedOperation.class, 4),

    START_TEST_PHASE(StartTestPhaseOperation.class, 5),

    START_TEST(StartTestOperation.class, 6),

    STOP_TEST(StopTestOperation.class, 7);

    private final Class<? extends SimulatorOperation> classType;
    private final int classId;

    OperationType(Class<? extends SimulatorOperation> classType, int classId) {
        this.classType = classType;
        this.classId = classId;

        OperationTypeRegistry.register(this, classType, classId);
    }

    public static OperationType getOperationType(SimulatorOperation operation) {
        OperationType operationType = OperationTypeRegistry.OPERATION_TYPES.get(operation.getClass());
        if (operationType == null) {
            throw new IllegalArgumentException(format("Operation %s has not been registered!", operation.getClass().getName()));
        }
        return operationType;
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

    static class OperationTypeRegistry {

        private static final ConcurrentMap<Integer, OperationType> CLASS_IDS = new ConcurrentHashMap<Integer, OperationType>();

        private static final ConcurrentMap<Class<? extends SimulatorOperation>, OperationType> OPERATION_TYPES
                = new ConcurrentHashMap<Class<? extends SimulatorOperation>, OperationType>();

        static void register(OperationType operationType, Class<? extends SimulatorOperation> classType, int classId) {
            if (classId < 0) {
                throw new IllegalArgumentException("classId must be a positive number");
            }

            OperationType oldType = CLASS_IDS.putIfAbsent(classId, operationType);
            if (oldType != null) {
                throw new IllegalStateException(format("classId %d is already registered to %s", classId, oldType));
            }

            oldType = OPERATION_TYPES.putIfAbsent(classType, operationType);
            if (oldType != null) {
                throw new IllegalStateException(format("classType %s is already registered to %s", classType.getName(), oldType));
            }
        }
    }
}
