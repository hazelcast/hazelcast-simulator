package com.hazelcast.stabilizer.common.messaging;

import java.io.Serializable;

public class Message implements Serializable {
    private MessageAddress messageAddress;

    public Message(MessageAddress messageAddress) {
        if (messageAddress == null) {
            throw new IllegalArgumentException("Message address cannot be null");
        }
        this.messageAddress = messageAddress;
    }

    public MessageAddress getMessageAddress() {
        return messageAddress;
    }
}
