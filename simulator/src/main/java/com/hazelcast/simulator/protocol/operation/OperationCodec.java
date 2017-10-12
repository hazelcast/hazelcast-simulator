/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.google.gson.Gson;
import com.hazelcast.simulator.protocol.core.SimulatorMessage;

/**
 * Encodes and decodes a {@link SimulatorOperation}.
 */
public final class OperationCodec {

    private static final Gson GSON = new Gson();

    private OperationCodec() {
    }

    public static String toJson(SimulatorOperation op) {
        return GSON.toJson(op);
    }

    public static SimulatorOperation fromJson(String json, Class<? extends SimulatorOperation> classType) {
        return GSON.fromJson(json, classType);
    }

    public static SimulatorOperation fromSimulatorMessage(SimulatorMessage message) {
        return fromJson(message.getOperationData(), message.getOperationType().getClassType());
    }
}
