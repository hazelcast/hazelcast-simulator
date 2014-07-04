package com.hazelcast.stabilizer.common.messaging;

import java.io.Serializable;

public class Message implements Serializable {
    private MessageAddress messageAddress;

    public Message(MessageAddress messageAddress) {
        this.messageAddress = messageAddress;
    }

    public MessageAddress getMessageAddress() {
        return messageAddress;
    }
}
