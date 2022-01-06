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
package com.hazelcast.simulator.worker.operations;

import com.google.gson.annotations.SerializedName;
import com.hazelcast.simulator.protocol.operation.SimulatorOperation;

/**
 * Initiates the shutdown process of the Worker.
 */
public class TerminateWorkerOperation implements SimulatorOperation {

    /**
     * If the worker should do a real shutdown or just fake it. This is needed for integration testing the code.
     *
     * If you are implementing a non java client? Just ignore this property and terminate your client.
     */
    @SerializedName("realShutdown")
    private final boolean realShutdown;

    public TerminateWorkerOperation(boolean realShutdown) {
        this.realShutdown = realShutdown;
    }

    public boolean isRealShutdown() {
        return realShutdown;
    }
}
