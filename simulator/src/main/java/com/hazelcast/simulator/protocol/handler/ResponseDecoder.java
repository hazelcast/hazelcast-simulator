package com.hazelcast.simulator.protocol.handler;

import com.hazelcast.simulator.protocol.core.Response;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.util.List;

import static com.hazelcast.simulator.protocol.core.ResponseCodec.decodeResponse;

/**
 * A {@link ByteToMessageDecoder} to decode a received {@link ByteBuf} to a {@link Response}.
 */
public class ResponseDecoder extends ByteToMessageDecoder {

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        Response response = decodeResponse(in);
        out.add(response);
    }
}
