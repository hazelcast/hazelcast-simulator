package com.hazelcast.simulator.protocol.handler;

import com.hazelcast.simulator.agent.workerjvm.WorkerJvm;
import com.hazelcast.simulator.protocol.core.AddressLevel;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.group.ChannelGroup;
import org.apache.log4j.Logger;

import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.simulator.protocol.core.SimulatorMessageCodec.getMessageId;
import static com.hazelcast.simulator.protocol.core.SimulatorMessageCodec.getSourceAddress;
import static com.hazelcast.simulator.protocol.core.SimulatorMessageCodec.isSimulatorMessage;
import static java.lang.String.format;

public class ForwardToCoordinatorHandler extends SimpleChannelInboundHandler<ByteBuf> {

    private static final Logger LOGGER = Logger.getLogger(ForwardToCoordinatorHandler.class);

    private final SimulatorAddress localAddress;
    private final AddressLevel addressLevel;

    private final ChannelGroup channelGroup;
    private final ConcurrentMap<SimulatorAddress, WorkerJvm> workerJVMs;

    public ForwardToCoordinatorHandler(SimulatorAddress localAddress, ChannelGroup channelGroup,
                                       ConcurrentMap<SimulatorAddress, WorkerJvm> workerJVMs) {
        this.localAddress = localAddress;
        this.addressLevel = localAddress.getAddressLevel();

        this.channelGroup = channelGroup;
        this.workerJVMs = workerJVMs;
    }

    @Override
    protected void channelRead0(final ChannelHandlerContext ctx, final ByteBuf buffer) throws Exception {
        if (isSimulatorMessage(buffer)) {
            long messageId = getMessageId(buffer);
            LOGGER.debug(format("[%d] %s %s forwarding message to parent", messageId, addressLevel, localAddress));

            updateLastSeenTimestamp(buffer);

            channelGroup.writeAndFlush(buffer.duplicate());
            buffer.retain();
        }
    }

    private void updateLastSeenTimestamp(ByteBuf buffer) {
        SimulatorAddress sourceAddress = getSourceAddress(buffer);
        AddressLevel addressLevel = sourceAddress.getAddressLevel();
        if (addressLevel == AddressLevel.WORKER) {
            updateLastSeenTimestamp(sourceAddress);
        } else if (addressLevel == AddressLevel.TEST) {
            updateLastSeenTimestamp(sourceAddress.getParent());
        }
    }

    private void updateLastSeenTimestamp(SimulatorAddress sourceAddress) {
        WorkerJvm workerJvm = workerJVMs.get(sourceAddress);
        if (workerJvm != null) {
            workerJvm.updateLastSeen();
        }
    }
}
