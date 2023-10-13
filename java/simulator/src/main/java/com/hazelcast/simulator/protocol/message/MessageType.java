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
package com.hazelcast.simulator.protocol.message;

import com.hazelcast.simulator.agent.messages.CreateWorkerMessage;
import com.hazelcast.simulator.agent.messages.StartTimeoutDetectionMessage;
import com.hazelcast.simulator.agent.messages.StopTimeoutDetectionMessage;
import com.hazelcast.simulator.coordinator.messages.FailureMessage;
import com.hazelcast.simulator.worker.messages.CreateTestMessage;
import com.hazelcast.simulator.worker.messages.ExecuteScriptMessage;
import com.hazelcast.simulator.worker.messages.PerformanceStatsMessage;
import com.hazelcast.simulator.worker.messages.StartPhaseMessage;
import com.hazelcast.simulator.worker.messages.StopRunMessage;
import com.hazelcast.simulator.worker.messages.TerminateWorkerMessage;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static java.lang.String.format;

/**
 * Defines the message type.
 */
public enum MessageType {

    // MessageProcessor
    LOG(LogMessage.class, 1),

    // Coordinator-Messages
    FAILURE(FailureMessage.class, 1000),
    PERFORMANCE_STATE(PerformanceStatsMessage.class, 1002),

    // Agent-Messages
    CREATE_WORKER(CreateWorkerMessage.class, 3000),
    START_TIMEOUT_DETECTION(StartTimeoutDetectionMessage.class, 3001),
    STOP_TIMEOUT_DETECTION(StopTimeoutDetectionMessage.class, 3002),

    // Worker-Messages
    TERMINATE_WORKER(TerminateWorkerMessage.class, 4001),
    CREATE_TEST(CreateTestMessage.class, 4002),
    EXECUTE_SCRIPT(ExecuteScriptMessage.class, 4003),
    START_TEST_PHASE(StartPhaseMessage.class, 4004),
    STOP_TEST(StopRunMessage.class, 4005);

    private final Class<? extends SimulatorMessage> classType;
    private final int classId;

    MessageType(Class<? extends SimulatorMessage> classType, int classId) {
        this.classType = classType;
        this.classId = classId;

        MessageTypeRegistry.register(this, classType, classId);
    }

    /**
     * Returns the {@link MessageType} of a {@link SimulatorMessage}.
     *
     * @param msg the {@link SimulatorMessage}
     * @return the {@link MessageType} of the {@link SimulatorMessage}
     */
    public static MessageType getMessageType(SimulatorMessage msg) {
        MessageType msgType = MessageTypeRegistry.TYPES.get(msg.getClass());
        if (msgType == null) {
            throw new IllegalArgumentException(
                    format("Message %s has not been registered!", msg.getClass().getName()));
        }
        return msgType;
    }

    /**
     * Returns the {@link MessageType} of a registered classId.
     *
     * @param classId the registered classId
     * @return the {@link MessageType}
     */
    public static MessageType fromInt(int classId) {
        MessageType msgType = MessageTypeRegistry.CLASS_IDS.get(classId);
        if (msgType == null) {
            throw new IllegalArgumentException(format("ClassId %d has not been registered!", classId));
        }
        return msgType;
    }

    /**
     * Returns the registered classId of a {@link MessageType}.
     *
     * @return the registered classId
     */
    public int toInt() {
        return classId;
    }

    /**
     * Returns the registered {@link Class} of the {@link MessageType} to deserialize a {@link SimulatorMessage}.
     *
     * @return the {@link Class} of the {@link MessageType}
     */
    public Class<? extends SimulatorMessage> getClassType() {
        return classType;
    }

    /**
     * Stores and validates the registered {@link MessageType} entries.
     * <p>
     * This class prevents double registration of class types or classIds, which
     * would produce late failures during runtime.
     */
    @SuppressWarnings("PMD.UnusedModifier")
    static class MessageTypeRegistry {

        private static final ConcurrentMap<Integer, MessageType> CLASS_IDS = new ConcurrentHashMap<>();
        private static final ConcurrentMap<Class<? extends SimulatorMessage>, MessageType> TYPES
                = new ConcurrentHashMap<>();

        static void register(MessageType msgType, Class<? extends SimulatorMessage> classType, int classId) {
            if (classId < 0) {
                throw new IllegalArgumentException("classId must be a positive number");
            }

            MessageType oldType = CLASS_IDS.putIfAbsent(classId, msgType);
            if (oldType != null) {
                throw new IllegalStateException(format("classId %d is already registered to %s", classId, oldType));
            }

            oldType = TYPES.putIfAbsent(classType, msgType);
            if (oldType != null) {
                throw new IllegalStateException(format("classType %s is already registered to %s", classType.getName(), oldType));
            }
        }
    }
}
