package com.hazelcast.simulator.protocol.handler;

import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.core.SimulatorMessage;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import org.apache.log4j.Logger;

import static com.hazelcast.simulator.protocol.core.SimulatorMessageCodec.encodeByteBuf;
import static java.lang.String.format;

/**
 * A {@link MessageToByteEncoder} to encode a {@link SimulatorMessage} to a {@link ByteBuf}.
 */
public class MessageEncoder extends MessageToByteEncoder<SimulatorMessage> {

    private static final Logger LOGGER = Logger.getLogger(MessageEncoder.class);

    private final SimulatorAddress localAddress;
    private final SimulatorAddress targetAddress;

    public MessageEncoder(SimulatorAddress localAddress, SimulatorAddress targetAddress) {
        this.localAddress = localAddress;
        this.targetAddress = targetAddress;
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, SimulatorMessage msg, ByteBuf out) throws Exception {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("[%d] MessageEncoder.encode() %s -> %s %s", msg.getMessageId(), localAddress, targetAddress,
                    msg));
        }
        encodeByteBuf(msg, out);
    }
}
