package com.hazelcast.simulator.protocol.handler;

import com.hazelcast.simulator.protocol.core.Response;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.core.SimulatorMessage;
import com.hazelcast.simulator.protocol.core.TestProcessorManager;
import com.hazelcast.simulator.protocol.processors.OperationProcessor;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.AttributeKey;
import org.apache.log4j.Logger;

import static com.hazelcast.simulator.protocol.operation.OperationCodec.fromSimulatorMessage;
import static java.lang.String.format;

/**
 * A {@link SimpleChannelInboundHandler} to to deserialize a {@link com.hazelcast.simulator.protocol.operation.SimulatorOperation}
 * from a received {@link SimulatorMessage} and execute it on the {@link OperationProcessor} of the addressed Simulator Test.
 */
public class MessageTestConsumeHandler extends SimpleChannelInboundHandler<SimulatorMessage> {

    private static final Logger LOGGER = Logger.getLogger(MessageTestConsumeHandler.class);

    private final AttributeKey<Integer> forwardAddressIndex = AttributeKey.valueOf("forwardAddressIndex");

    private final TestProcessorManager testProcessorManager;
    private final SimulatorAddress localAddress;

    public MessageTestConsumeHandler(TestProcessorManager testProcessorManager, SimulatorAddress localAddress) {
        this.testProcessorManager = testProcessorManager;
        this.localAddress = localAddress;
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, SimulatorMessage msg) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("[%d] %s MessageTestConsumeHandler is consuming message...", msg.getMessageId(), localAddress));
        }

        Response response = new Response(msg);
        int testAddressIndex = ctx.attr(forwardAddressIndex).get();
        if (testAddressIndex == 0) {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace(format("[%d] forwarding message to all tests", msg.getMessageId()));
            }
            testProcessorManager.processOnAllTests(response, fromSimulatorMessage(msg), msg.getSource());
        } else {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace(format("[%d] forwarding message to test %d", msg.getMessageId(), testAddressIndex));
            }
            testProcessorManager.processOnTest(response, fromSimulatorMessage(msg), msg.getSource(), testAddressIndex);
        }
        ctx.writeAndFlush(response);
    }
}
