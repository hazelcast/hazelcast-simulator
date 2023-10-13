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

import com.google.gson.Gson;

/**
 * Encodes and decodes a {@link SimulatorMessage}.
 */
public final class SimulatorMessageCodec {

    private static final Gson GSON = new Gson();

    private SimulatorMessageCodec() {
    }

    public static String toJson(SimulatorMessage msg) {
        return GSON.toJson(msg);
    }

    public static SimulatorMessage fromJson(String json, Class<? extends SimulatorMessage> classType) {
        return GSON.fromJson(json, classType);
    }
}
