package com.hazelcast.simulator.protocol.handler;

import com.hazelcast.simulator.protocol.core.SimulatorMessage;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import static com.hazelcast.simulator.protocol.core.SimulatorMessageCodec.encodeByteBuf;

/**
 * A {@link MessageToByteEncoder} to encode a {@link SimulatorMessage} to a {@link ByteBuf}.
 */
public class MessageEncoder extends MessageToByteEncoder<SimulatorMessage> {

    @Override
    protected void encode(ChannelHandlerContext ctx, SimulatorMessage msg, ByteBuf out) throws Exception {
        encodeByteBuf(msg, out);
    }
}
