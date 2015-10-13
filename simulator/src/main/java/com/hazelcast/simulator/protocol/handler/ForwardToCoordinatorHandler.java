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
            if (LOGGER.isTraceEnabled()) {
                long messageId = getMessageId(buffer);
                LOGGER.trace(format("[%d] %s %s forwarding message to parent", messageId, addressLevel, localAddress));
            }

            updateWorkerJvmLastSeenTimestamp(buffer);

            buffer.retain();
            channelGroup.writeAndFlush(buffer);
        }
    }

    private void updateWorkerJvmLastSeenTimestamp(ByteBuf buffer) {
        SimulatorAddress sourceAddress = getSourceAddress(buffer);
        AddressLevel sourceAddressLevel = sourceAddress.getAddressLevel();
        if (sourceAddressLevel == AddressLevel.WORKER) {
            updateWorkerJvmLastSeenTimestamp(sourceAddress);
        } else if (sourceAddressLevel == AddressLevel.TEST) {
            updateWorkerJvmLastSeenTimestamp(sourceAddress.getParent());
        }
    }

    private void updateWorkerJvmLastSeenTimestamp(SimulatorAddress sourceAddress) {
        WorkerJvm workerJvm = workerJVMs.get(sourceAddress);
        if (workerJvm != null) {
            workerJvm.updateLastSeen();
        }
    }
}
