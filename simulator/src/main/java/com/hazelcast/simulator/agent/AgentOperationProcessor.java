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
package com.hazelcast.simulator.agent;

import com.hazelcast.simulator.agent.operations.CreateWorkerOperation;
import com.hazelcast.simulator.agent.operations.StartTimeoutDetectionOperation;
import com.hazelcast.simulator.agent.operations.StopTimeoutDetectionOperation;
import com.hazelcast.simulator.agent.workerprocess.WorkerProcessFailureMonitor;
import com.hazelcast.simulator.agent.workerprocess.WorkerProcessManager;
import com.hazelcast.simulator.protocol.OperationProcessor;
import com.hazelcast.simulator.protocol.Promise;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.exception.ProcessException;
import com.hazelcast.simulator.protocol.operation.SimulatorOperation;

import static com.hazelcast.simulator.protocol.core.ResponseType.SUCCESS;
import static com.hazelcast.simulator.protocol.core.ResponseType.UNSUPPORTED_OPERATION_ON_THIS_PROCESSOR;

class AgentOperationProcessor implements OperationProcessor {

    private final WorkerProcessManager processManager;
    private final WorkerProcessFailureMonitor failureMonitor;

    public AgentOperationProcessor(WorkerProcessManager processManager,
                                   WorkerProcessFailureMonitor failureMonitor) {
        this.processManager = processManager;
        this.failureMonitor = failureMonitor;
    }

    @Override
    public void process(SimulatorOperation op, SimulatorAddress source, Promise promise) throws Exception {
        if (op instanceof CreateWorkerOperation) {
            processManager.launch((CreateWorkerOperation) op, promise);
        } else if (op instanceof StartTimeoutDetectionOperation) {
            failureMonitor.startTimeoutDetection();
            promise.answer(SUCCESS);
        } else if (op instanceof StopTimeoutDetectionOperation) {
            failureMonitor.stopTimeoutDetection();
            promise.answer(SUCCESS);
        } else {
            throw new ProcessException("Unknown operation:" + op, UNSUPPORTED_OPERATION_ON_THIS_PROCESSOR);
        }
    }
}
