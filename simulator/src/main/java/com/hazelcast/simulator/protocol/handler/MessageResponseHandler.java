package com.hazelcast.simulator.protocol.handler;

import com.hazelcast.simulator.protocol.core.MessageFuture;
import com.hazelcast.simulator.protocol.core.Response;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.apache.log4j.Logger;

import java.util.concurrent.ConcurrentMap;

import static java.lang.String.format;

/**
 * A {@link SimpleChannelInboundHandler} to set a received {@link Response} as result of the corresponding {@link MessageFuture}.
 */
public class MessageResponseHandler extends SimpleChannelInboundHandler<Response> {

    private static final Logger LOGGER = Logger.getLogger(MessageResponseHandler.class);

    private final int addressIndex;
    private final ConcurrentMap<String, MessageFuture<Response>> futureMap;

    public MessageResponseHandler(int addressIndex, ConcurrentMap<String, MessageFuture<Response>> futureMap) {
        this.addressIndex = addressIndex;
        this.futureMap = futureMap;
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, Response response) {
        if (Response.isLastResponse(response)) {
            return;
        }
        String futureKey = response.getMessageId() + "_" + addressIndex;
        MessageFuture<Response> future = futureMap.get(futureKey);
        if (future != null) {
            future.set(response);
            return;
        }
        LOGGER.error(format("%s: No future found for %s", futureKey, response));
    }
}
