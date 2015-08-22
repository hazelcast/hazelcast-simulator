package com.hazelcast.simulator.protocol.handler;

import com.hazelcast.simulator.protocol.core.AddressLevel;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.core.SimulatorMessage;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.util.AttributeKey;
import org.apache.log4j.Logger;

import java.util.List;

import static com.hazelcast.simulator.protocol.core.SimulatorMessageCodec.decodeSimulatorMessage;
import static com.hazelcast.simulator.protocol.core.SimulatorMessageCodec.getChildAddressIndex;
import static com.hazelcast.simulator.protocol.core.SimulatorMessageCodec.getDestinationAddressLevel;
import static com.hazelcast.simulator.protocol.core.SimulatorMessageCodec.getMessageId;
import static io.netty.buffer.Unpooled.EMPTY_BUFFER;
import static java.lang.String.format;

/**
 * A {@link ByteToMessageDecoder} to decode a received {@link ByteBuf} to a {@link SimulatorMessage}.
 *
 * If the {@link SimulatorMessage} has a child as destination {@link SimulatorAddress},
 * the {@link ByteBuf} is passed to the next handler.
 */
public class MessageDecoder extends ByteToMessageDecoder {

    private static final Logger LOGGER = Logger.getLogger(MessageDecoder.class);

    private final AttributeKey<Integer> forwardAddressIndex = AttributeKey.valueOf("forwardAddressIndex");

    private final SimulatorAddress localAddress;
    private final AddressLevel addressLevel;
    private final int addressLevelValue;

    public MessageDecoder(SimulatorAddress localAddress, AddressLevel addressLevel) {
        this.localAddress = localAddress;
        this.addressLevel = addressLevel;
        this.addressLevelValue = addressLevel.toInt();
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        if (EMPTY_BUFFER.equals(in)) {
            return;
        }
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("[%d] MessageDecoder.decode() %s %s", 0, addressLevel, localAddress));
        }

        long messageId = getMessageId(in);
        AddressLevel dstAddressLevel = AddressLevel.fromInt(getDestinationAddressLevel(in));
        LOGGER.debug(format("[%d] %s %s received a message for addressLevel %s", messageId, addressLevel, localAddress,
                dstAddressLevel));

        if (dstAddressLevel == addressLevel) {
            SimulatorMessage message = decodeSimulatorMessage(in);
            out.add(message);
        } else {
            int addressIndex = getChildAddressIndex(in, addressLevelValue);
            ctx.attr(forwardAddressIndex).set(addressIndex);

            out.add(in.duplicate());
            in.readerIndex(in.readableBytes());
            in.retain();
        }
    }
}
