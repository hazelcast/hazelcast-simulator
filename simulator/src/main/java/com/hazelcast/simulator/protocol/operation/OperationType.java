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
package com.hazelcast.simulator.protocol.operation;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static java.lang.String.format;

/**
 * Defines the operation type for a {@link com.hazelcast.simulator.protocol.core.SimulatorMessage}.
 */
public enum OperationType {

    // OperationProcessor
    INTEGRATION_TEST(IntegrationTestOperation.class, 0),
    LOG(LogOperation.class, 1),
    CHAOS_MONKEY(ChaosMonkeyOperation.class, 2),

    // CoordinatorOperationProcessor
    EXCEPTION(ExceptionOperation.class, 3),
    FAILURE(FailureOperation.class, 4),
    PHASE_COMPLETED(PhaseCompletedOperation.class, 5),
    PERFORMANCE_STATE(PerformanceStateOperation.class, 6),
    TEST_HISTOGRAMS(TestHistogramOperation.class, 7),

    // AgentOperationProcessor
    INIT_TEST_SUITE(InitTestSuiteOperation.class, 8),
    CREATE_WORKER(CreateWorkerOperation.class, 9),
    START_TIMEOUT_DETECTION(StartTimeoutDetectionOperation.class, 10),
    STOP_TIMEOUT_DETECTION(StopTimeoutDetectionOperation.class, 11),

    // WorkerOperationProcessor
    PING(PingOperation.class, 12),
    TERMINATE_WORKER(TerminateWorkerOperation.class, 13),
    CREATE_TEST(CreateTestOperation.class, 14),

    // TestOperationProcessor
    START_TEST_PHASE(StartTestPhaseOperation.class, 15),
    START_TEST(StartTestOperation.class, 16),
    STOP_TEST(StopTestOperation.class, 17);

    private final Class<? extends SimulatorOperation> classType;
    private final int classId;

    OperationType(Class<? extends SimulatorOperation> classType, int classId) {
        this.classType = classType;
        this.classId = classId;

        OperationTypeRegistry.register(this, classType, classId);
    }

    /**
     * Returns the {@link OperationType} of a {@link SimulatorOperation}.
     *
     * @param operation the {@link SimulatorOperation}
     * @return the {@link OperationType} of the {@link SimulatorOperation}
     */
    public static OperationType getOperationType(SimulatorOperation operation) {
        OperationType operationType = OperationTypeRegistry.OPERATION_TYPES.get(operation.getClass());
        if (operationType == null) {
            throw new IllegalArgumentException(format("Operation %s has not been registered!", operation.getClass().getName()));
        }
        return operationType;
    }

    /**
     * Returns the {@link OperationType} of a registered classId.
     *
     * @param classId the registered classId
     * @return the {@link OperationType}
     */
    public static OperationType fromInt(int classId) {
        OperationType operationType = OperationTypeRegistry.CLASS_IDS.get(classId);
        if (operationType == null) {
            throw new IllegalArgumentException(format("ClassId %d has not been registered!", classId));
        }
        return operationType;
    }

    /**
     * Returns the registered classId of a {@link OperationType}.
     *
     * @return the registered classId
     */
    public int toInt() {
        return classId;
    }

    /**
     * Returns the registered {@link Class} of the {@link OperationType} to deserialize a {@link SimulatorOperation}.
     *
     * @return the {@link Class} of the {@link OperationType}
     */
    public Class<? extends SimulatorOperation> getClassType() {
        return classType;
    }

    /**
     * Stores and validates the registered {@link OperationType} entries.
     *
     * This class prevents double registration of class types or classIds, which would produce late failures during runtime.
     */
    @SuppressWarnings("PMD.UnusedModifier")
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
