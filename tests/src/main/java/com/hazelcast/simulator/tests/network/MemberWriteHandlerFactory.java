package com.hazelcast.simulator.tests.network;

import com.hazelcast.nio.tcp.MemberWriteHandler;
import com.hazelcast.nio.tcp.WriteHandler;

public class MemberWriteHandlerFactory implements WriteHandlerFactory {

    @Override
    public WriteHandler create() {
        return new MemberWriteHandler();
    }
}
