package com.hazelcast.simulator.protocol.connector;

import com.hazelcast.simulator.protocol.configuration.ClientConfiguration;
import com.hazelcast.simulator.protocol.core.Response;
import com.hazelcast.simulator.protocol.core.ResponseFuture;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.core.SimulatorMessage;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.log4j.Logger;

import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.simulator.protocol.configuration.ServerConfiguration.DEFAULT_SHUTDOWN_QUIET_PERIOD;
import static com.hazelcast.simulator.protocol.configuration.ServerConfiguration.DEFAULT_SHUTDOWN_TIMEOUT;
import static com.hazelcast.simulator.protocol.core.ResponseFuture.createFutureKey;
import static com.hazelcast.simulator.protocol.core.ResponseFuture.createInstance;
import static com.hazelcast.simulator.protocol.core.SimulatorMessageCodec.getMessageId;
import static com.hazelcast.simulator.protocol.core.SimulatorMessageCodec.getSourceAddress;
import static java.lang.String.format;

/**
 * Client connector for a Simulator Coordinator or Agent.
 */
public class ClientConnector {

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
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(group)
                .channel(NioSocketChannel.class)
                .remoteAddress(new InetSocketAddress(configuration.getRemoteHost(), configuration.getRemotePort()))
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel channel) {
                        configuration.configurePipeline(channel.pipeline());
                    }
                });

        ChannelFuture future = bootstrap.connect().syncUninterruptibly();
        channel = future.channel();

        LOGGER.info(format("ClientConnector %s -> %s sends to %s", configuration.getLocalAddress(),
                configuration.getRemoteAddress(), channel.remoteAddress()));
    }

    public void shutdown() {
        group.shutdownGracefully(DEFAULT_SHUTDOWN_QUIET_PERIOD, DEFAULT_SHUTDOWN_TIMEOUT, TimeUnit.SECONDS).syncUninterruptibly();
    }

    public void forwardToChannel(ByteBuf buffer) {
        channel.writeAndFlush(buffer);
    }

    public Response write(SimulatorMessage message) throws Exception {
        return writeAsync(message).get();
    }

    public Response write(ByteBuf buffer) throws Exception {
        return writeAsync(buffer).get();
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
}
