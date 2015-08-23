package com.hazelcast.simulator.protocol.handler;

import com.hazelcast.simulator.protocol.core.MessageFuture;
import com.hazelcast.simulator.protocol.core.Response;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.apache.log4j.Logger;

import java.util.concurrent.ConcurrentMap;

import static java.lang.String.format;

/**
 * A {@link SimpleChannelInboundHandler} to set a received {@link Response} as result of the corresponding {@link MessageFuture}.
 */
public class ResponseHandler extends SimpleChannelInboundHandler<Response> {

    private static final Logger LOGGER = Logger.getLogger(ResponseHandler.class);

    private final SimulatorAddress localAddress;
    private final SimulatorAddress childAddress;
    private final int childIndex;
    private final ConcurrentMap<String, MessageFuture<Response>> futureMap;

    public ResponseHandler(SimulatorAddress localAddress, SimulatorAddress childAddress,
                           ConcurrentMap<String, MessageFuture<Response>> futureMap) {
        this.localAddress = localAddress;
        this.childAddress = childAddress;
        this.childIndex = childAddress.getAddressIndex();
        this.futureMap = futureMap;
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, Response response) {
        if (Response.isLastResponse(response)) {
            return;
        }
        long messageId = response.getMessageId();
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("[%d] ResponseHandler.channelRead0() %s -> %s", messageId, localAddress, childAddress));
        }

        String futureKey = messageId + "_" + childIndex;
        MessageFuture<Response> future = futureMap.get(futureKey);
        if (future != null) {
            future.set(response);
            return;
        }

        String msg = format("[%d] %s -> %s No future found for %s", messageId, localAddress, childAddress, response);
        LOGGER.error(msg);
        throw new IllegalArgumentException(msg);
    }
}
