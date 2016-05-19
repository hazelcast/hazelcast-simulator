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
 * Control operation from a remote controller to the Coordinator.
 */
public class RemoteControllerOperation implements SimulatorOperation {

    public enum Type {
        INTEGRATION_TEST,
        RESPONSE,
        LIST_COMPONENTS
    }

    /**
     * Defines the {@link IntegrationTestOperation.Type} of this operation.
     */
    private final String type;

    /**
     * Defines the payload of this operation.
     */
    private final String payload;

    public RemoteControllerOperation(Type type) {
        this(type, null);
    }

    public RemoteControllerOperation(Type type, String payload) {
        this.type = type.name();
        this.payload = payload;
    }

    public Type getType() {
        return Type.valueOf(type);
    }

    public String getPayload() {
        return payload;
    }
}
