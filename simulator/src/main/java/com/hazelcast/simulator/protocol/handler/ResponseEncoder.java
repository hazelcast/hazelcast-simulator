package com.hazelcast.simulator.protocol.handler;

import com.hazelcast.simulator.protocol.core.Response;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import org.apache.log4j.Logger;

import static com.hazelcast.simulator.protocol.core.ResponseCodec.encodeByteBuf;
import static java.lang.String.format;

/**
 * A {@link MessageToByteEncoder} to encode a {@link Response} to a {@link ByteBuf}.
 */
public class ResponseEncoder extends MessageToByteEncoder<Response> {

    private static final Logger LOGGER = Logger.getLogger(ResponseEncoder.class);

    private final SimulatorAddress localAddress;

    public ResponseEncoder(SimulatorAddress localAddress) {
        this.localAddress = localAddress;
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, Response response, ByteBuf out) throws Exception {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("[%d] ResponseEncoder.encode() %s %s", response.getMessageId(), localAddress, response));
        }
        encodeByteBuf(response, out);
    }
}
