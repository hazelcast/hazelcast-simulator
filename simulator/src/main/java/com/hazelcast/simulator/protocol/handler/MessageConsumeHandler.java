package com.hazelcast.simulator.protocol.handler;

import com.hazelcast.simulator.protocol.core.AddressLevel;
import com.hazelcast.simulator.protocol.core.Response;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.core.SimulatorMessage;
import com.hazelcast.simulator.protocol.processors.OperationProcessor;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.apache.log4j.Logger;

import static com.hazelcast.simulator.protocol.operation.OperationHandler.processMessage;
import static java.lang.String.format;

/**
 * A {@link SimpleChannelInboundHandler} to deserialize a {@link com.hazelcast.simulator.protocol.operation.SimulatorOperation}
 * from a received {@link SimulatorMessage} and execute it on the configured {@link OperationProcessor}.
 */
public class MessageConsumeHandler extends SimpleChannelInboundHandler<SimulatorMessage> {

    private static final Logger LOGGER = Logger.getLogger(MessageConsumeHandler.class);

    private final SimulatorAddress localAddress;
    private final AddressLevel addressLevel;

    private final OperationProcessor processor;

    public MessageConsumeHandler(SimulatorAddress localAddress, OperationProcessor processor) {
        this.localAddress = localAddress;
        this.addressLevel = localAddress.getAddressLevel();

        this.processor = processor;
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, SimulatorMessage msg) {
        long messageId = msg.getMessageId();
        LOGGER.debug(format("[%d] %s %s MessageConsumeHandler is consuming message...", messageId, addressLevel, localAddress));

        ctx.writeAndFlush(new Response(messageId, msg.getSource(), localAddress, processMessage(msg, processor)));
    }
}
