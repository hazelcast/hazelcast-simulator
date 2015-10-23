package com.hazelcast.simulator.protocol.connector;

import com.hazelcast.simulator.protocol.configuration.ClientConfiguration;
import com.hazelcast.simulator.protocol.core.Response;
import com.hazelcast.simulator.protocol.core.ResponseFuture;
import com.hazelcast.simulator.protocol.core.ResponseType;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.core.SimulatorMessage;
import com.hazelcast.simulator.protocol.core.SimulatorProtocolException;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.log4j.Logger;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.simulator.protocol.configuration.ServerConfiguration.DEFAULT_SHUTDOWN_QUIET_PERIOD;
import static com.hazelcast.simulator.protocol.configuration.ServerConfiguration.DEFAULT_SHUTDOWN_TIMEOUT;
import static com.hazelcast.simulator.protocol.core.ResponseFuture.createFutureKey;
import static com.hazelcast.simulator.protocol.core.ResponseFuture.createInstance;
import static com.hazelcast.simulator.protocol.core.ResponseFuture.getMessageIdFromFutureKey;
import static com.hazelcast.simulator.protocol.core.ResponseFuture.getSourceFromFutureKey;
import static com.hazelcast.simulator.protocol.core.SimulatorMessageCodec.getMessageId;
import static com.hazelcast.simulator.protocol.core.SimulatorMessageCodec.getSourceAddress;
import static java.lang.String.format;

/**
 * Client connector for a Simulator Coordinator or Agent.
 */
public class ClientConnector {

    private static final int CONNECT_TIMEOUT_MILLIS = (int) TimeUnit.MINUTES.toMillis(2);

    private static final Logger LOGGER = Logger.getLogger(ClientConnector.class);

    private final EventLoopGroup group = new NioEventLoopGroup();

    private final ClientConfiguration configuration;
    private final ConcurrentMap<String, ResponseFuture> futureMap;

    private Channel channel;

    public ClientConnector(ClientConfiguration configuration) {
        this.configuration = configuration;
        this.futureMap = configuration.getFutureMap();
    }

    public void start() {
        Bootstrap bootstrap = getBootstrap();
        ChannelFuture future = bootstrap.connect().syncUninterruptibly();
        channel = future.channel();

        LOGGER.info(format("ClientConnector %s -> %s sends to %s", configuration.getLocalAddress(),
                configuration.getRemoteAddress(), channel.remoteAddress()));
    }

    private Bootstrap getBootstrap() {
        Bootstrap bootstrap = new Bootstrap();
        bootstrap
                .group(group)
                .channel(NioSocketChannel.class)
                .remoteAddress(new InetSocketAddress(configuration.getRemoteHost(), configuration.getRemotePort()))
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, CONNECT_TIMEOUT_MILLIS)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel channel) {
                        configuration.configurePipeline(channel.pipeline());
                    }
                });
        return bootstrap;
    }

    public void shutdown() {
        if (channel.isOpen()) {
            channel.close().syncUninterruptibly();
        }
        group.shutdownGracefully(DEFAULT_SHUTDOWN_QUIET_PERIOD, DEFAULT_SHUTDOWN_TIMEOUT, TimeUnit.SECONDS).syncUninterruptibly();

        // take care about eventually pending ResponseFuture instances
        for (Map.Entry<String, ResponseFuture> futureEntry : futureMap.entrySet()) {
            String futureKey = futureEntry.getKey();
            LOGGER.warn(format("ResponseFuture %s still running after shutdown!", futureKey));
            Response response = new Response(getMessageIdFromFutureKey(futureKey), getSourceFromFutureKey(futureKey));
            response.addResponse(configuration.getLocalAddress(), ResponseType.EXCEPTION_DURING_OPERATION_EXECUTION);
            futureEntry.getValue().set(response);
        }
    }

    public ClientConfiguration getConfiguration() {
        return configuration;
    }

    public void forwardToChannel(ByteBuf buffer) {
        channel.writeAndFlush(buffer);
    }

    public Response write(SimulatorMessage message) {
        ResponseFuture future = writeAsync(message);
        return getResponse(future);
    }

    public Response write(ByteBuf buffer) {
        ResponseFuture future = writeAsync(buffer);
        return getResponse(future);
    }

    public ResponseFuture writeAsync(SimulatorMessage message) {
        return writeAsync(message.getSource(), message.getMessageId(), message);
    }

    public ResponseFuture writeAsync(ByteBuf buffer) {
        return writeAsync(getSourceAddress(buffer), getMessageId(buffer), buffer);
    }

    private ResponseFuture writeAsync(SimulatorAddress source, long messageId, Object msg) {
        String futureKey = createFutureKey(source, messageId, configuration.getRemoteIndex());
        ResponseFuture future = createInstance(futureMap, futureKey);
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("[%d] %s created ResponseFuture %s", messageId, configuration.getLocalAddress(), futureKey));
        }
        channel.writeAndFlush(msg);

        return future;
    }

    private Response getResponse(ResponseFuture future) {
        try {
            return future.get();
        } catch (InterruptedException e) {
            throw new SimulatorProtocolException("ResponseFuture.get() got interrupted!", e);
        }
    }
}
