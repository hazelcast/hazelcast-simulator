package com.hazelcast.simulator.protocol.handler;

import com.hazelcast.simulator.protocol.core.AddressLevel;
import com.hazelcast.simulator.protocol.core.Response;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.util.CharsetUtil;
import org.apache.log4j.Logger;

import java.util.List;

import static com.hazelcast.simulator.protocol.core.ResponseCodec.decodeResponse;
import static java.lang.String.format;

/**
 * A {@link ByteToMessageDecoder} to decode a received {@link ByteBuf} to a {@link Response}.
 */
public class ResponseDecoder extends ByteToMessageDecoder {

    private static final Logger LOGGER = Logger.getLogger(ResponseDecoder.class);

    private final SimulatorAddress localAddress;
    private final AddressLevel addressLevel;
    private final int addressIndex;

    public ResponseDecoder(SimulatorAddress localAddress, AddressLevel addressLevel, int addressIndex) {
        this.localAddress = localAddress;
        this.addressLevel = addressLevel;
        this.addressIndex = addressIndex;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        Response response = decodeResponse(in);
        if (LOGGER.isTraceEnabled() && response != Response.LAST_RESPONSE) {
            LOGGER.trace(format("[%d] ResponseDecoder.decode() %s %s_%s %s", response.getMessageId(), localAddress,
                    addressLevel, addressIndex, in.toString(CharsetUtil.UTF_8)));
        }
        out.add(response);
    }
}
