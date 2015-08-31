package com.hazelcast.simulator.tests.network;

import com.hazelcast.nio.tcp.MemberSocketWriter;
import com.hazelcast.nio.tcp.SocketWriter;

public class MemberSocketWriterFactory implements SocketWriterFactory {

    @Override
    public SocketWriter create() {
        return new MemberSocketWriter();
    }
}
