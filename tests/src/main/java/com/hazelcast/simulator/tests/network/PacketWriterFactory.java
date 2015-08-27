package com.hazelcast.simulator.tests.network;

import com.hazelcast.nio.tcp.PacketWriter;

public interface PacketWriterFactory  {

    PacketWriter create();
}
