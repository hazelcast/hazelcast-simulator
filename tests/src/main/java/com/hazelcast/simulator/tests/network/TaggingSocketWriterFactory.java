package com.hazelcast.simulator.tests.network;


import com.hazelcast.nio.tcp.SocketWriter;

class TaggingSocketWriterFactory implements SocketWriterFactory {

    @Override
    public SocketWriter create() {
        return new TaggingPacketSocketWriter();
    }
}
