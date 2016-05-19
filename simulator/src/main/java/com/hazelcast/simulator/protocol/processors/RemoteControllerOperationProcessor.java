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
package com.hazelcast.simulator.protocol.processors;

import com.hazelcast.simulator.protocol.core.ResponseType;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.exception.ExceptionLogger;
import com.hazelcast.simulator.protocol.operation.OperationType;
import com.hazelcast.simulator.protocol.operation.RemoteControllerOperation;
import com.hazelcast.simulator.protocol.operation.SimulatorOperation;

import static com.hazelcast.simulator.protocol.core.ResponseType.SUCCESS;
import static com.hazelcast.simulator.protocol.core.ResponseType.UNSUPPORTED_OPERATION_ON_THIS_PROCESSOR;

/**
 * An {@link OperationProcessor} implementation to process {@link RemoteControllerOperation} instances
 * from a Simulator Coordinator on a Simulator Remote Controller.
 */
public class RemoteControllerOperationProcessor extends AbstractOperationProcessor {

    private volatile String response;

    public RemoteControllerOperationProcessor(ExceptionLogger exceptionLogger) {
        super(exceptionLogger);
    }

    public String getResponse() {
        return response;
    }

    @Override
    public final ResponseType processOperation(OperationType operationType, SimulatorOperation operation,
                                               SimulatorAddress sourceAddress) {
        switch (operationType) {
            case REMOTE_CONTROLLER:
                return processRemoteController((RemoteControllerOperation) operation);
            default:
                return UNSUPPORTED_OPERATION_ON_THIS_PROCESSOR;
        }
    }

    private ResponseType processRemoteController(RemoteControllerOperation operation) {
        switch (operation.getType()) {
            case RESPONSE:
                response = operation.getPayload();
                break;
            default:
                return UNSUPPORTED_OPERATION_ON_THIS_PROCESSOR;
        }
        return SUCCESS;
    }
}
