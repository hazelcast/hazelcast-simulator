package com.hazelcast.simulator.tests.network;

import com.hazelcast.nio.tcp.MemberPacketWriter;
import com.hazelcast.nio.tcp.PacketWriter;

public class MemberPacketWriterFactory implements PacketWriterFactory {
    @Override
    public PacketWriter create() {
        return new MemberPacketWriter();
    }
}
