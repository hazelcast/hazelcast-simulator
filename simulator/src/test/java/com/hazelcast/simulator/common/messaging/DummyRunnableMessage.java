package com.hazelcast.simulator.common.messaging;

import com.hazelcast.simulator.common.KeyValuePair;

@SuppressWarnings("unused")
@MessageSpec(value = "dummyMessage", description = "Dummy Runnable Message")
public class DummyRunnableMessage extends RunnableMessage {

    private final KeyValuePair<String, String> attribute;

    private volatile boolean executed = false;

    public DummyRunnableMessage(MessageAddress messageAddress) {
        this(messageAddress, null);
    }

    public DummyRunnableMessage(MessageAddress messageAddress, KeyValuePair<String, String> attribute) {
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
