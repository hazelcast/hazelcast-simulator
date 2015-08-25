package com.hazelcast.simulator.protocol.handler;

import com.google.gson.Gson;
import com.hazelcast.simulator.protocol.core.AddressLevel;
import com.hazelcast.simulator.protocol.core.Response;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.core.SimulatorMessage;
import com.hazelcast.simulator.protocol.operation.SimulatorOperation;
import com.hazelcast.simulator.protocol.operation.SimulatorOperationFactory;
import com.hazelcast.simulator.protocol.processors.OperationProcessor;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.apache.log4j.Logger;

import static java.lang.String.format;

/**
 * A {@link SimpleChannelInboundHandler} to deserialize a {@link SimulatorOperation} from a received {@link SimulatorMessage}
 * and execute it on the configured {@link OperationProcessor}.
 */
public class MessageConsumeHandler extends SimpleChannelInboundHandler<SimulatorMessage> {

    private static final Logger LOGGER = Logger.getLogger(MessageConsumeHandler.class);

    private final Gson gson = new Gson();

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

        SimulatorOperation operation = SimulatorOperationFactory.fromJson(gson, msg);
        ctx.writeAndFlush(new Response(messageId, msg.getSource(), localAddress, processor.process(operation)));
    }
}
