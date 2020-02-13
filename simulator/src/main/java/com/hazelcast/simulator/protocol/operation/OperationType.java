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

import com.hazelcast.simulator.agent.operations.CreateWorkerOperation;
import com.hazelcast.simulator.agent.operations.StartTimeoutDetectionOperation;
import com.hazelcast.simulator.agent.operations.StopTimeoutDetectionOperation;
import com.hazelcast.simulator.coordinator.operations.FailureOperation;
import com.hazelcast.simulator.coordinator.operations.RcDownloadOperation;
import com.hazelcast.simulator.coordinator.operations.RcInstallOperation;
import com.hazelcast.simulator.coordinator.operations.RcPrintLayoutOperation;
import com.hazelcast.simulator.coordinator.operations.RcStopCoordinatorOperation;
import com.hazelcast.simulator.coordinator.operations.RcTestRunOperation;
import com.hazelcast.simulator.coordinator.operations.RcTestStatusOperation;
import com.hazelcast.simulator.coordinator.operations.RcTestStopOperation;
import com.hazelcast.simulator.coordinator.operations.RcWorkerKillOperation;
import com.hazelcast.simulator.coordinator.operations.RcWorkerScriptOperation;
import com.hazelcast.simulator.coordinator.operations.RcWorkerStartOperation;
import com.hazelcast.simulator.worker.operations.CreateTestOperation;
import com.hazelcast.simulator.worker.operations.ExecuteScriptOperation;
import com.hazelcast.simulator.worker.operations.PerformanceStatsOperation;
import com.hazelcast.simulator.worker.operations.StartPhaseOperation;
import com.hazelcast.simulator.worker.operations.StopRunOperation;
import com.hazelcast.simulator.worker.operations.TerminateWorkerOperation;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static java.lang.String.format;

/**
 * Defines the operation type.
 */
public enum OperationType {

    // OperationProcessor
    LOG(LogOperation.class, 1),

    // Coordinator-Operations
    FAILURE(FailureOperation.class, 1000),
    PERFORMANCE_STATE(PerformanceStatsOperation.class, 1002),

    // Coordinator Remote operations
    RC_INSTALL(RcInstallOperation.class, 2000),
    RC_EXIT(RcStopCoordinatorOperation.class, 2001),
    RC_TEST_RUN(RcTestRunOperation.class, 2002),
    RC_TEST_STATUS(RcTestStatusOperation.class, 2003),
    RC_TEST_STOP(RcTestStopOperation.class, 2004),
    RC_WORKER_KILL(RcWorkerKillOperation.class, 2005),
    RC_WORKER_SCRIPT(RcWorkerScriptOperation.class, 2006),
    RC_WORKER_START(RcWorkerStartOperation.class, 2007),
    RC_PRINT_LAYOUT(RcPrintLayoutOperation.class, 2008),
    RC_DOWNLOAD(RcDownloadOperation.class, 2009),

    // Agent-Operations
    CREATE_WORKER(CreateWorkerOperation.class, 3000),
    START_TIMEOUT_DETECTION(StartTimeoutDetectionOperation.class, 3001),
    STOP_TIMEOUT_DETECTION(StopTimeoutDetectionOperation.class, 3002),

    // Worker-Operations
    TERMINATE_WORKER(TerminateWorkerOperation.class, 4001),
    CREATE_TEST(CreateTestOperation.class, 4002),
    EXECUTE_SCRIPT(ExecuteScriptOperation.class, 4003),
    START_TEST_PHASE(StartPhaseOperation.class, 4004),
    STOP_TEST(StopRunOperation.class, 4005);

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
     * @param op the {@link SimulatorOperation}
     * @return the {@link OperationType} of the {@link SimulatorOperation}
     */
    public static OperationType getOperationType(SimulatorOperation op) {
        OperationType operationType = OperationTypeRegistry.OPERATION_TYPES.get(op.getClass());
        if (operationType == null) {
            throw new IllegalArgumentException(format("Operation %s has not been registered!", op.getClass().getName()));
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
     * <p>
     * This class prevents double registration of class types or classIds, which would produce late failures during runtime.
     */
    @SuppressWarnings("PMD.UnusedModifier")
    static class OperationTypeRegistry {

        private static final ConcurrentMap<Integer, OperationType> CLASS_IDS = new ConcurrentHashMap<>();
        private static final ConcurrentMap<Class<? extends SimulatorOperation>, OperationType> OPERATION_TYPES
                = new ConcurrentHashMap<>();

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
