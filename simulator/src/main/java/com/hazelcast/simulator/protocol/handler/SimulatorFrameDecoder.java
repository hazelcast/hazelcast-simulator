package com.hazelcast.simulator.protocol.handler;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

import static com.hazelcast.simulator.protocol.core.BaseCodec.INT_SIZE;

/**
 * A decoder that splits the received {@link ByteBuf}s dynamically by the value of the length field in the message.
 */
public class SimulatorFrameDecoder extends LengthFieldBasedFrameDecoder {

    public SimulatorFrameDecoder() {
        super(Integer.MAX_VALUE, 0, INT_SIZE);
    }
}
