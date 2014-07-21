package com.hazelcast.stabilizer.common.messaging;

import com.hazelcast.stabilizer.common.KeyValuePair;

import java.io.Serializable;

public class AttributeMessage<K extends Serializable, V extends Serializable> extends Message {

    private KeyValuePair<K, V> attribute;

    public AttributeMessage(MessageAddress messageAddress,
                            KeyValuePair<K, V> attribute) {
        super(messageAddress);
        this.attribute = attribute;
    }

    public KeyValuePair<K, V> getAttribute() {
        return attribute;
    }
}
