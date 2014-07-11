package com.hazelcast.stabilizer.common.messaging;

import com.hazelcast.stabilizer.common.KeyValuePair;

import java.io.Serializable;

@MessageSpec("dummyMessage")
public class DummyRunnableMessage extends RunnableMessage {

    private volatile boolean executed = false;
    private final KeyValuePair<String, String> attribute;

    public DummyRunnableMessage(MessageAddress messageAddress) {
        this(messageAddress, null);
    }

    public DummyRunnableMessage(MessageAddress messageAddress,
                                KeyValuePair<String, String> attribute) {
        super(messageAddress);
        this.attribute = attribute;
    }

    public KeyValuePair<String, String> getAttribute() {
        return attribute;
    }

    @Override
    public void run() {
        executed = true;
    }

    public boolean isExecuted() {
        return executed;
    }
}
