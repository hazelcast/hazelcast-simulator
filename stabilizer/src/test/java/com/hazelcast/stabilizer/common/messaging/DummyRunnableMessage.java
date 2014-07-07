package com.hazelcast.stabilizer.common.messaging;

@MessageSpec("dummyMessage")
public class DummyRunnableMessage extends RunnableMessage {

    private volatile boolean executed = false;

    public DummyRunnableMessage(MessageAddress messageAddress) {
        super(messageAddress);
    }

    @Override
    public void run() {
        executed = true;
    }

    public boolean isExecuted() {
        return executed;
    }
}
