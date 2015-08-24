package com.hazelcast.simulator.protocol.connector;

import com.hazelcast.simulator.protocol.configuration.ServerConfiguration;
import com.hazelcast.simulator.protocol.core.Response;
import com.hazelcast.simulator.protocol.core.ResponseFuture;
import com.hazelcast.simulator.protocol.core.SimulatorMessage;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.apache.log4j.Logger;

import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.simulator.protocol.configuration.ServerConfiguration.DEFAULT_SHUTDOWN_QUIET_PERIOD;
import static com.hazelcast.simulator.protocol.configuration.ServerConfiguration.DEFAULT_SHUTDOWN_TIMEOUT;
import static java.lang.String.format;

/**
 * Server connector class for a Simulator Agent or Worker.
 */
class ServerConnector {

    private static final Logger LOGGER = Logger.getLogger(ServerConnector.class);

    private final EventLoopGroup group = new NioEventLoopGroup();

    private final ServerConfiguration configuration;
    private final ConcurrentMap<String, ResponseFuture> futureMap;

    public ServerConnector(ServerConfiguration configuration) {
        this.configuration = configuration;
        this.futureMap = configuration.getFutureMap();
    }

    public void start() {
        ServerBootstrap bootstrap = new ServerBootstrap();
        bootstrap.group(group)
                .channel(NioServerSocketChannel.class)
                .localAddress(new InetSocketAddress(configuration.getLocalPort()))
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel channel) {
                        configuration.configurePipeline(channel.pipeline());
                    }
                });

        ChannelFuture future = bootstrap.bind().syncUninterruptibly();
        Channel channel = future.channel();

        LOGGER.info(format("ServerConnector %s listens on %s", configuration.getLocalAddress(), channel.localAddress()));
    }

    public void shutdown() {
        group.shutdownGracefully(DEFAULT_SHUTDOWN_QUIET_PERIOD, DEFAULT_SHUTDOWN_TIMEOUT, TimeUnit.SECONDS).syncUninterruptibly();
    }

    public Response write(SimulatorMessage message) throws Exception {
        return writeAsync(message).get();
    }

    private ResponseFuture writeAsync(SimulatorMessage message) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("[%d] ServerConnector.writeAsync() %s", message.getMessageId(), message));
        }
        return writeAsync(message.getMessageId(), message);
    }

    private ResponseFuture writeAsync(long messageId, Object msg) {
        String futureKey = configuration.createFutureKey(messageId);
        ResponseFuture future = ResponseFuture.createInstance(futureMap, futureKey);
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("[%d] %s created ResponseFuture %s", messageId, configuration.getLocalAddress(), futureKey));
        }
        configuration.getChannelGroup().writeAndFlush(msg);

        return future;
    }
}
