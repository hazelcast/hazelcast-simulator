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

/**
 * Starts a ChaosMonkey on the target.
 */
public class ChaosMonkeyOperation implements SimulatorOperation {

    public enum Type {
        INTEGRATION_TEST,
        BLOCK_TRAFFIC,
        UNBLOCK_TRAFFIC,
        SPIN_CORE_INDEFINITELY,
        USE_ALL_MEMORY,
        SOFT_KILL,
        HARD_KILL
    }

    /**
     * {@link Type} of this operation.
     */
    private final String type;

    public ChaosMonkeyOperation(Type type) {
        this.type = type.name();
    }

    public Type getType() {
        return Type.valueOf(type);
    }
}
