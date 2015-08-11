package com.hazelcast.simulator.protocol.handler;

import com.hazelcast.simulator.protocol.connector.ClientConnector;
import com.hazelcast.simulator.protocol.core.AddressLevel;
import com.hazelcast.simulator.protocol.core.Response;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.AttributeKey;
import org.apache.log4j.Logger;

import java.util.concurrent.ConcurrentHashMap;

import static com.hazelcast.simulator.protocol.core.ResponseType.FAILURE_WORKER_NOT_FOUND;
import static com.hazelcast.simulator.protocol.core.SimulatorMessageCodec.getMessageId;
import static java.lang.String.format;

/**
 * A {@link SimpleChannelInboundHandler} to forward a received {@link ByteBuf} to a connected Simulator Worker.
 */
public class MessageForwardToWorkerHandler extends SimpleChannelInboundHandler<ByteBuf> {

    private static final Logger LOGGER = Logger.getLogger(MessageEncoder.class);

    private final AttributeKey<Integer> workerAddressIndex = AttributeKey.valueOf("addressIndex");

    private final ConcurrentHashMap<Integer, ClientConnector> worker = new ConcurrentHashMap<Integer, ClientConnector>();

    private final SimulatorAddress localAddress;
    private final AddressLevel addressLevel;

    public MessageForwardToWorkerHandler(SimulatorAddress localAddress) {
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
        long messageId = getMessageId(buffer);

        int workerAddressIndex = ctx.attr(this.workerAddressIndex).get();
        Response response = new Response(messageId);
        if (workerAddressIndex == 0) {
            LOGGER.debug(format("[%d] %s forwarding message to all workers", messageId, addressLevel));
            for (ClientConnector clientConnector : worker.values()) {
                buffer.retain();
                response.addResponse(clientConnector.write(buffer));
            }
        } else {
            ClientConnector clientConnector = worker.get(workerAddressIndex);
            if (clientConnector == null) {
                LOGGER.debug(format("[%d] %s worker %d not found!", messageId, addressLevel, workerAddressIndex));
                ctx.writeAndFlush(new Response(messageId, localAddress, FAILURE_WORKER_NOT_FOUND));
                return;
            }
            LOGGER.debug(format("[%d] %s forwarding message to worker %d", messageId, addressLevel, workerAddressIndex));
            buffer.retain();
            response.addResponse(clientConnector.write(buffer));
        }
        ctx.writeAndFlush(response);
    }
}
