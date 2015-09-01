package com.hazelcast.simulator.tests.network;

import com.hazelcast.nio.tcp.WriteHandler;

public interface WriteHandlerFactory {

    WriteHandler create();
}
