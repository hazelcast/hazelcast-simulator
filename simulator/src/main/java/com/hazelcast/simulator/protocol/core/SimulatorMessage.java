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
package com.hazelcast.simulator.protocol.core;

import com.hazelcast.simulator.protocol.operation.OperationType;

/**
 * Message with a JSON serialized {@link com.hazelcast.simulator.protocol.operation.SimulatorOperation} which can be sent
 * from any Simulator component to another.
 */
public class SimulatorMessage {

    private final SimulatorAddress destination;
    private final SimulatorAddress source;
    private final long messageId;

    private final OperationType operationType;
    private final String operationData;

    public SimulatorMessage(SimulatorAddress destination, SimulatorAddress source, long messageId,
                            OperationType operationType, String operationData) {
        this.destination = destination;
        this.source = source;
        this.messageId = messageId;
        this.operationType = operationType;
        this.operationData = operationData;
    }

    public SimulatorAddress getDestination() {
        return destination;
    }

    public SimulatorAddress getSource() {
        return source;
    }

    public long getMessageId() {
        return messageId;
    }

    public OperationType getOperationType() {
        return operationType;
    }

    public String getOperationData() {
        return operationData;
    }

    @Override
    public String toString() {
        return "SimulatorMessage{"
                + "destination=" + destination
                + ", source=" + source
                + ", messageId=" + messageId
                + ", operationType=" + operationType
                + ", operationData='" + operationData + '\''
                + '}';
    }
}
