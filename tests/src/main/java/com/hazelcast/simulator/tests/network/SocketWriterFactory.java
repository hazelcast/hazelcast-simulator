package com.hazelcast.simulator.tests.network;

import com.hazelcast.nio.tcp.SocketWriter;

public interface SocketWriterFactory {

    SocketWriter create();
}
