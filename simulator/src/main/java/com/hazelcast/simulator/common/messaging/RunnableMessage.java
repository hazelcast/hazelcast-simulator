package com.hazelcast.simulator.common.messaging;

public abstract class RunnableMessage extends Message implements Runnable {

    public RunnableMessage(MessageAddress messageAddress) {
        super(messageAddress);
    }
}
