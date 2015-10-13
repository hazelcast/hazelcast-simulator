package com.hazelcast.simulator.protocol.handler;

import com.hazelcast.simulator.protocol.core.Response;
import com.hazelcast.simulator.protocol.core.ResponseType;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.core.SimulatorMessage;
import com.hazelcast.simulator.protocol.processors.OperationProcessor;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.AttributeKey;
import org.apache.log4j.Logger;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.simulator.protocol.core.AddressLevel.TEST;
import static com.hazelcast.simulator.protocol.core.ResponseType.FAILURE_TEST_NOT_FOUND;
import static com.hazelcast.simulator.protocol.operation.OperationCodec.fromSimulatorMessage;
import static java.lang.String.format;

/**
 * A {@link SimpleChannelInboundHandler} to to deserialize a {@link com.hazelcast.simulator.protocol.operation.SimulatorOperation}
 * from a received {@link SimulatorMessage} and execute it on the {@link OperationProcessor} of the addressed Simulator Test.
 */
public class MessageTestConsumeHandler extends SimpleChannelInboundHandler<SimulatorMessage> {

    private static final Logger LOGGER = Logger.getLogger(MessageTestConsumeHandler.class);

    private final AttributeKey<Integer> forwardAddressIndex = AttributeKey.valueOf("forwardAddressIndex");
    private final ConcurrentMap<Integer, SimulatorAddress> testAddresses = new ConcurrentHashMap<Integer, SimulatorAddress>();
    private final ConcurrentMap<Integer, OperationProcessor> testProcessors
            = new ConcurrentHashMap<Integer, OperationProcessor>();

    private final SimulatorAddress localAddress;
    private final int agentIndex;
    private final int workerIndex;

    public MessageTestConsumeHandler(SimulatorAddress localAddress) {
        this.localAddress = localAddress;
        this.agentIndex = localAddress.getAgentIndex();
        this.workerIndex = localAddress.getWorkerIndex();
    }

    public void addTest(int testIndex, OperationProcessor processor) {
        SimulatorAddress testAddress = new SimulatorAddress(TEST, agentIndex, workerIndex, testIndex);
        testAddresses.put(testIndex, testAddress);
        testProcessors.put(testIndex, processor);
    }

    public void removeTest(int testIndex) {
        testAddresses.remove(testIndex);
        testProcessors.remove(testIndex);
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
            for (Map.Entry<Integer, OperationProcessor> entry : testProcessors.entrySet()) {
                ResponseType responseType = entry.getValue().process(fromSimulatorMessage(msg));
                response.addResponse(testAddresses.get(entry.getKey()), responseType);
            }
        } else {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace(format("[%d] forwarding message to test %d", msg.getMessageId(), testAddressIndex));
            }
            OperationProcessor processor = testProcessors.get(testAddressIndex);
            if (processor == null) {
                response.addResponse(localAddress, FAILURE_TEST_NOT_FOUND);
            } else {
                ResponseType responseType = processor.process(fromSimulatorMessage(msg));
                response.addResponse(testAddresses.get(testAddressIndex), responseType);
            }
        }
        ctx.writeAndFlush(response);
    }
}
