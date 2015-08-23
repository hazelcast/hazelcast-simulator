package com.hazelcast.simulator.protocol.handler;

import com.hazelcast.simulator.protocol.core.Response;
import com.hazelcast.simulator.protocol.core.ResponseFuture;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.apache.log4j.Logger;

import java.util.concurrent.ConcurrentMap;

import static java.lang.String.format;

/**
 * A {@link SimpleChannelInboundHandler} to set a received {@link Response} as result of the corresponding {@link ResponseFuture}.
 */
public class ResponseHandler extends SimpleChannelInboundHandler<Response> {

    private static final Logger LOGGER = Logger.getLogger(ResponseHandler.class);

    private final SimulatorAddress localAddress;
    private final SimulatorAddress remoteAddress;
    private final int senderIndex;
    private final ConcurrentMap<String, ResponseFuture> futureMap;

    public ResponseHandler(SimulatorAddress localAddress, SimulatorAddress remoteAddress,
                           ConcurrentMap<String, ResponseFuture> futureMap) {
        this.localAddress = localAddress;
        this.remoteAddress = remoteAddress;
        this.senderIndex = remoteAddress.getAddressIndex();
        this.futureMap = futureMap;
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, Response response) {
        if (Response.isLastResponse(response)) {
            return;
        }
        long messageId = response.getMessageId();
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("[%d] ResponseHandler.channelRead0() %s <- %s", messageId, localAddress, remoteAddress));
        }

        String futureKey = messageId + "_" + senderIndex;
        ResponseFuture future = futureMap.get(futureKey);
        if (future != null) {
            future.set(response);
            return;
        }

        String msg = format("[%d] %s <- %s No future found for %s", messageId, localAddress, remoteAddress, response);
        LOGGER.error(msg);
        throw new IllegalArgumentException(msg);
    }
}
