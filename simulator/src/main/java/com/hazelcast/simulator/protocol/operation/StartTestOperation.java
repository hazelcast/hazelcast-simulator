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

import com.google.gson.annotations.SerializedName;
import com.hazelcast.simulator.common.TestPhase;
import com.hazelcast.simulator.common.WorkerType;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.registry.TargetType;

import java.util.Collections;
import java.util.List;

/**
 * Starts the {@link TestPhase#RUN} phase of a Simulator Test.
 */
public class StartTestOperation implements SimulatorOperation {

    /**
     * Defines which Workers should execute the RUN phase by their {@link WorkerType}.
     */
    @SerializedName("targetType")
    private final TargetType targetType;

    /**
     * Defines which Workers should execute the RUN phase by their {@link SimulatorAddress}.
     */
    @SerializedName("targetWorkers")
    private final List<String> targetWorkers;

    public StartTestOperation() {
        this(TargetType.ALL);
    }

    public StartTestOperation(TargetType targetType) {
        this(targetType, Collections.<String>emptyList());
    }

    public StartTestOperation(TargetType targetType, List<String> targetWorkers) {
        this.targetType = targetType;
        this.targetWorkers = targetWorkers;
    }

    public boolean matchesTargetType(WorkerType workerType) {
        return targetType.matches(workerType.isMember());
    }

    public boolean matchesTargetWorkers(SimulatorAddress workerAddress) {
        return (targetWorkers.isEmpty() || targetWorkers.contains(workerAddress.toString()));
    }

    public TargetType getTargetType() {
        return targetType;
    }
}
