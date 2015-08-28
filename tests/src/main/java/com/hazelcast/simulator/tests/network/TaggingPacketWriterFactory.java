package com.hazelcast.simulator.tests.network;

import com.hazelcast.nio.tcp.PacketWriter;

class TaggingPacketWriterFactory implements PacketWriterFactory {

    @Override
    public PacketWriter create() {
        return new TaggingPacketWriter();
    }
}
