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
