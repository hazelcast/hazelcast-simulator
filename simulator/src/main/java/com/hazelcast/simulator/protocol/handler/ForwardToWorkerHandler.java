package com.hazelcast.simulator.protocol.handler;

import com.hazelcast.simulator.protocol.connector.ClientConnector;
import com.hazelcast.simulator.protocol.core.AddressLevel;
import com.hazelcast.simulator.protocol.core.Response;
import com.hazelcast.simulator.protocol.core.ResponseCodec;
import com.hazelcast.simulator.protocol.core.ResponseFuture;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.core.SimulatorMessageCodec;
import com.hazelcast.simulator.protocol.core.SimulatorProtocolException;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.AttributeKey;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import static com.hazelcast.simulator.protocol.core.ResponseCodec.isResponse;
import static com.hazelcast.simulator.protocol.core.ResponseType.FAILURE_WORKER_NOT_FOUND;
import static com.hazelcast.simulator.protocol.core.SimulatorMessageCodec.getSourceAddress;
import static com.hazelcast.simulator.protocol.core.SimulatorMessageCodec.isSimulatorMessage;
import static java.lang.String.format;

/**
 * A {@link SimpleChannelInboundHandler} to forward a received {@link ByteBuf} to a connected Simulator Worker.
 */
public class ForwardToWorkerHandler extends SimpleChannelInboundHandler<ByteBuf> {

    private static final Logger LOGGER = Logger.getLogger(ForwardToWorkerHandler.class);

    private final AttributeKey<Integer> forwardAddressIndex = AttributeKey.valueOf("forwardAddressIndex");

    private final ConcurrentHashMap<Integer, ClientConnector> worker = new ConcurrentHashMap<Integer, ClientConnector>();

    private final SimulatorAddress localAddress;
    private final AddressLevel addressLevel;

    public ForwardToWorkerHandler(SimulatorAddress localAddress) {
        this.localAddress = localAddress;
        this.addressLevel = localAddress.getAddressLevel();
    }

    public void addWorker(int workerIndex, ClientConnector clientConnector) {
        worker.put(workerIndex, clientConnector);
    }

    public void removeWorker(int workerIndex) {
        ClientConnector clientConnector = worker.remove(workerIndex);
        if (clientConnector != null) {
            clientConnector.shutdown();
        }
    }

    @Override
    protected void channelRead0(final ChannelHandlerContext ctx, final ByteBuf buffer) throws Exception {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("ForwardToWorkerHandler.channelRead0() %s %s", addressLevel, localAddress));
        }

        int workerAddressIndex = ctx.attr(forwardAddressIndex).get();
        if (isSimulatorMessage(buffer)) {
            forwardSimulatorMessage(ctx, buffer, workerAddressIndex);
        } else if (isResponse(buffer)) {
            forwardResponse(ctx, buffer, workerAddressIndex);
        }
    }

    private void forwardSimulatorMessage(ChannelHandlerContext ctx, ByteBuf buffer, int workerAddressIndex) {
        long messageId = SimulatorMessageCodec.getMessageId(buffer);

        Response response = new Response(messageId, getSourceAddress(buffer));
        if (workerAddressIndex == 0) {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace(format("[%d] %s forwarding message to all workers", messageId, addressLevel));
            }
            List<ResponseFuture> futureList = new ArrayList<ResponseFuture>();
            for (ClientConnector clientConnector : worker.values()) {
                buffer.retain();
                futureList.add(clientConnector.writeAsync(buffer));
            }
            try {
                for (ResponseFuture future : futureList) {
                    response.addResponse(future.get());
                }
            } catch (InterruptedException e) {
                throw new SimulatorProtocolException("ResponseFuture.get() got interrupted!", e);
            }
        } else {
            ClientConnector clientConnector = worker.get(workerAddressIndex);
            if (clientConnector == null) {
                LOGGER.error(format("[%d] %s worker %d not found!", messageId, addressLevel, workerAddressIndex));
                response.addResponse(localAddress, FAILURE_WORKER_NOT_FOUND);
                ctx.writeAndFlush(response);
                return;
            }
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace(format("[%d] %s forwarding message to worker %d", messageId, addressLevel, workerAddressIndex));
            }
            buffer.retain();
            response.addResponse(clientConnector.write(buffer));
        }
        ctx.writeAndFlush(response);
    }

    private void forwardResponse(ChannelHandlerContext ctx, ByteBuf buffer, int workerAddressIndex) {
        long messageId = ResponseCodec.getMessageId(buffer);

        ClientConnector clientConnector = worker.get(workerAddressIndex);
        if (clientConnector == null) {
            LOGGER.error(format("[%d] %s worker %d not found!", messageId, addressLevel, workerAddressIndex));
            ctx.writeAndFlush(new Response(messageId, localAddress, localAddress, FAILURE_WORKER_NOT_FOUND));
            return;
        }

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("[%d] %s forwarding response to worker %d", messageId, addressLevel, workerAddressIndex));
        }
        buffer.retain();
        clientConnector.forwardToChannel(buffer);
    }
}
