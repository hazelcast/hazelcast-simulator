package com.hazelcast.stabilizer.agent.remoting;

import com.hazelcast.stabilizer.common.messaging.MessageAddress;
import com.hazelcast.stabilizer.common.messaging.RunnableMessage;

public class DummyRunnableMessage extends RunnableMessage {

    private volatile boolean executed = false;

    DummyRunnableMessage(MessageAddress messageAddress) {
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
