package com.hazelcast.simulator.protocol.core;

/**
 * Message with a JSON serialized {@link com.hazelcast.simulator.protocol.operation.SimulatorOperation} which can be sent
 * from any Simulator component to another.
 */
public class SimulatorMessage {

    private final SimulatorAddress destination;
    private final SimulatorAddress source;
    private final long messageId;

    private final int messageType;
    private final String messageData;

    public SimulatorMessage(SimulatorAddress destination, SimulatorAddress source, long messageId,
                            int messageType, String messageData) {
        this.destination = destination;
        this.source = source;
        this.messageId = messageId;
        this.messageType = messageType;
        this.messageData = messageData;
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

    public int getMessageType() {
        return messageType;
    }

    public String getMessageData() {
        return messageData;
    }
}
