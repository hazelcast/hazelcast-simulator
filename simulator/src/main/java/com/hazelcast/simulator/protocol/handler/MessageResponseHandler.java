package com.hazelcast.simulator.protocol.handler;

import com.hazelcast.simulator.protocol.core.AddressLevel;
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
public class MessageResponseHandler extends SimpleChannelInboundHandler<Response> {

    private static final Logger LOGGER = Logger.getLogger(MessageResponseHandler.class);

    private final SimulatorAddress localAddress;
    private final AddressLevel addressLevel;
    private final int addressIndex;
    private final ConcurrentMap<String, MessageFuture<Response>> futureMap;

    public MessageResponseHandler(SimulatorAddress localAddress, AddressLevel addressLevel, int addressIndex,
                                  ConcurrentMap<String, MessageFuture<Response>> futureMap) {
        this.localAddress = localAddress;
        this.addressLevel = addressLevel;
        this.addressIndex = addressIndex;
        this.futureMap = futureMap;
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, Response response) {
        if (Response.isLastResponse(response)) {
            return;
        }
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("[%d] MessageResponseHandler.channelRead0() %s %s_%s", response.getMessageId(), localAddress,
                    addressLevel, addressIndex));
        }

        String futureKey = response.getMessageId() + "_" + addressIndex;
        MessageFuture<Response> future = futureMap.get(futureKey);
        if (future != null) {
            future.set(response);
            return;
        }

        LOGGER.error(format("[%d] %s %s_%s No future found for %s of %s", response.getMessageId(), localAddress,
                addressLevel, addressIndex, futureKey, response));
    }
}
