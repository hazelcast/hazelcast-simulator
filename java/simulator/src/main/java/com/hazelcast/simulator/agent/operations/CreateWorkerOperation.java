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
package com.hazelcast.simulator.agent.operations;

import com.hazelcast.simulator.agent.workerprocess.WorkerParameters;
import com.hazelcast.simulator.protocol.operation.SimulatorOperation;

/**
 * Creates one or more Simulator Workers, based on a list of {@link WorkerParameters}.
 */
public class CreateWorkerOperation implements SimulatorOperation {

    // a list containing WorkerParameters. Each item in the list represent a single worker to be created.
    private final WorkerParameters workerParameters;
    private int delayMs;

    public CreateWorkerOperation(WorkerParameters workerParameters, int delayMs) {
        this.workerParameters = workerParameters;
        this.delayMs = delayMs;
    }

    public int getDelayMs() {
        return delayMs;
    }

    public WorkerParameters getWorkerParameters() {
        return workerParameters;
    }
}
