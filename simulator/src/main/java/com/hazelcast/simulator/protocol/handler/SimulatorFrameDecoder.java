package com.hazelcast.simulator.protocol.handler;

import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

import static com.hazelcast.simulator.protocol.core.BaseCodec.INT_SIZE;

/**
 * Splits the received {@link io.netty.buffer.ByteBuf}s dynamically by the value of the length field in the message.
 */
public class SimulatorFrameDecoder extends LengthFieldBasedFrameDecoder {

    private static final int MAX_FRAME_SIZE = Integer.MAX_VALUE;
    private static final int LENGTH_FIELD_OFFSET = 0;
    private static final int LENGTH_FIELD_SIZE = INT_SIZE;

    public SimulatorFrameDecoder() {
        super(MAX_FRAME_SIZE, LENGTH_FIELD_OFFSET, LENGTH_FIELD_SIZE);
    }
}
