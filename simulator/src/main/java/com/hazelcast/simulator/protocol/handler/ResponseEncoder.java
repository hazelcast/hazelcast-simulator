package com.hazelcast.simulator.protocol.handler;

import com.hazelcast.simulator.protocol.core.Response;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import static com.hazelcast.simulator.protocol.core.ResponseCodec.encodeByteBuf;

/**
 * A {@link MessageToByteEncoder} to encode a {@link Response} to a {@link ByteBuf}.
 */
public class ResponseEncoder extends MessageToByteEncoder<Response> {

    @Override
    protected void encode(ChannelHandlerContext ctx, Response response, ByteBuf out) throws Exception {
        encodeByteBuf(response, out);
    }
}
