package com.hazelcast.simulator.tests.network;

import com.hazelcast.nio.tcp.WriteHandler;

class TaggingWriteHandlerFactory implements WriteHandlerFactory {

    @Override
    public WriteHandler create() {
        return new TaggingPacketWriteHandler();
    }
}
