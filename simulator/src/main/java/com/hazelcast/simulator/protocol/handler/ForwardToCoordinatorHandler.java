package com.hazelcast.simulator.protocol.handler;

import com.hazelcast.simulator.protocol.core.AddressLevel;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.core.SimulatorMessageCodec;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.group.ChannelGroup;
import org.apache.log4j.Logger;

import static com.hazelcast.simulator.protocol.core.SimulatorMessageCodec.getMessageId;
import static java.lang.String.format;

public class ForwardToCoordinatorHandler extends SimpleChannelInboundHandler<ByteBuf> {

    private static final Logger LOGGER = Logger.getLogger(ForwardToCoordinatorHandler.class);

    private final SimulatorAddress localAddress;
    private final AddressLevel addressLevel;

    private final ChannelGroup channelGroup;

    public ForwardToCoordinatorHandler(SimulatorAddress localAddress, ChannelGroup channelGroup) {
        this.localAddress = localAddress;
        this.addressLevel = localAddress.getAddressLevel();

        this.channelGroup = channelGroup;
    }

    @Override
    protected void channelRead0(final ChannelHandlerContext ctx, final ByteBuf buffer) throws Exception {
        if (SimulatorMessageCodec.isSimulatorMessage(buffer)) {
            long messageId = getMessageId(buffer);
            LOGGER.debug(format("[%d] %s %s forwarding message to parent", messageId, addressLevel, localAddress));

            channelGroup.writeAndFlush(buffer.duplicate());
            buffer.retain();
        }
    }
}
