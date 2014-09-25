package com.hazelcast.stabilizer.common.messaging;

public abstract class RunnableMessage extends Message implements Runnable {

    public RunnableMessage(MessageAddress messageAddress) {
        super(messageAddress);
    }
}
