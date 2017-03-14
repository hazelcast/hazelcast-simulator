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
package com.hazelcast.simulator.protocol.core;

import com.hazelcast.simulator.protocol.OperationProcessor;
import com.hazelcast.simulator.protocol.operation.SimulatorOperation;

/**
 * Defines the type for a {@link Response}, e.g. if it was successful or a specific error occurred.
 */
public enum ResponseType {

    /**
     * Is returned when the {@link SimulatorMessage} was correctly processed.
     */
    SUCCESS(0),

    /**
     * Is returned when the addressed Coordinator was not found by an Agent component.
     */
    FAILURE_COORDINATOR_NOT_FOUND(1),

    /**
     * Is returned when the addressed Agent was not found by the Coordinator.
     */
    FAILURE_AGENT_NOT_FOUND(2),

    /**
     * Is returned when the addressed Worker was not found by an Agent component.
     */
    FAILURE_WORKER_NOT_FOUND(3),

    /**
     * Is returned when the addressed Test was not found by a Worker component.
     */
    FAILURE_TEST_NOT_FOUND(4),

    /**
     * Is returned when an implementation of {@link OperationProcessor}
     * does not implement the transmitted {@link SimulatorOperation}.
     */
    UNSUPPORTED_OPERATION_ON_THIS_PROCESSOR(5),

    /**
     * Is returned when an exception occurs during the execution of a
     * {@link SimulatorOperation}.
     */
    EXCEPTION_DURING_OPERATION_EXECUTION(6);

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
