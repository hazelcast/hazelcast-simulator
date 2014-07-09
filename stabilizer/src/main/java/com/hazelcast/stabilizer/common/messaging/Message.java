package com.hazelcast.stabilizer.common.messaging;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public abstract class Message implements Serializable {
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

    public static Message newBySpec(String messageTypeSpec, String messageAddressSpec) {
        return MessagesFactory.bySpec(messageTypeSpec, messageAddressSpec);
    }

}
