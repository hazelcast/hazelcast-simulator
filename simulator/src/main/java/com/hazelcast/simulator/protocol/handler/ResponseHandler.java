package com.hazelcast.simulator.protocol.handler;

import com.hazelcast.simulator.protocol.core.Response;
import com.hazelcast.simulator.protocol.core.ResponseFuture;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.apache.log4j.Logger;

import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.simulator.protocol.core.ResponseFuture.createFutureKey;
import static java.lang.String.format;

/**
 * A {@link SimpleChannelInboundHandler} to set a received {@link Response} as result of the corresponding {@link ResponseFuture}.
 */
public class ResponseHandler extends SimpleChannelInboundHandler<Response> {

    private static final Logger LOGGER = Logger.getLogger(ResponseHandler.class);

    private final SimulatorAddress localAddress;
    private final SimulatorAddress remoteAddress;

    private final ConcurrentMap<String, ResponseFuture> futureMap;
    private final int futureKeyIndex;

    public ResponseHandler(SimulatorAddress localAddress, SimulatorAddress remoteAddress,
                           ConcurrentMap<String, ResponseFuture> futureMap) {
        this(localAddress, remoteAddress, futureMap, remoteAddress.getAddressIndex());
    }

    public ResponseHandler(SimulatorAddress localAddress, SimulatorAddress remoteAddress,
                           ConcurrentMap<String, ResponseFuture> futureMap, int futureKeyIndex) {
        this.localAddress = localAddress;
        this.remoteAddress = remoteAddress;

        this.futureMap = futureMap;
        this.futureKeyIndex = futureKeyIndex;
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, Response response) {
        long messageId = response.getMessageId();
        String key = createFutureKey(response.getDestination(), messageId, futureKeyIndex);
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("[%d] %s <- %s received %s for %s", messageId, localAddress, remoteAddress, response, key));
        }

        ResponseFuture future = futureMap.get(key);
        if (future != null) {
            future.set(response);
            return;
        }

        String msg = format("[%d] %s <- %s futureKey %s not found for %s", messageId, localAddress, remoteAddress, key, response);
        LOGGER.error(msg);
        throw new IllegalArgumentException(msg);
    }
}
