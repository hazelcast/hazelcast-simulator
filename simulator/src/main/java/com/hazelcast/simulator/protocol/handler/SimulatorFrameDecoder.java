package com.hazelcast.simulator.protocol.handler;

import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

import static com.hazelcast.simulator.protocol.core.BaseCodec.INT_SIZE;

/**
 * Splits the received {@link io.netty.buffer.ByteBuf}s dynamically by the value of the length field in the message.
 */
public class SimulatorFrameDecoder extends LengthFieldBasedFrameDecoder {

    public SimulatorFrameDecoder() {
        super(Integer.MAX_VALUE, 0, INT_SIZE);
    }
}
