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
package com.hazelcast.simulator.protocol.processors;

import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.exception.ProcessException;
import com.hazelcast.simulator.protocol.operation.OperationType;
import com.hazelcast.simulator.protocol.operation.SimulatorOperation;
import com.hazelcast.simulator.worker.Promise;

import static com.hazelcast.simulator.protocol.core.ResponseType.UNSUPPORTED_OPERATION_ON_THIS_PROCESSOR;

/**
 * An {@link OperationProcessor} implementation to process Operations
 * from a Simulator Coordinator on a CoordinatorRemote.
 */
public class CoordinatorRemoteOperationProcessor extends AbstractOperationProcessor {

    @Override
    public final void processOperation(OperationType operationType, SimulatorOperation op,
                                       SimulatorAddress sourceAddress, Promise promise) {
        switch (operationType) {
            default:
                throw new ProcessException(UNSUPPORTED_OPERATION_ON_THIS_PROCESSOR);
        }
    }
}
