package com.hazelcast.simulator.protocol.connector;

import com.hazelcast.simulator.protocol.configuration.ServerConfiguration;
import com.hazelcast.simulator.protocol.core.MessageFuture;
import com.hazelcast.simulator.protocol.core.Response;
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
import java.util.concurrent.ConcurrentHashMap;
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

    private final ConcurrentMap<String, MessageFuture<Response>> futureMap
            = new ConcurrentHashMap<String, MessageFuture<Response>>();

    private final ServerConfiguration configuration;

    public ServerConnector(ServerConfiguration configuration) {
        this.configuration = configuration;
    }

    public void start() {
        ServerBootstrap bootstrap = new ServerBootstrap();
        bootstrap.group(group)
                .channel(NioServerSocketChannel.class)
                .localAddress(new InetSocketAddress(configuration.getLocalPort()))
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel channel) {
                        configuration.configurePipeline(channel.pipeline(), futureMap);
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

    private MessageFuture<Response> writeAsync(SimulatorMessage message) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("[%d] ServerConnector.writeAsync() %s", message.getMessageId(), message));
        }
        return writeAsync(message.getMessageId(), message);
    }

    private MessageFuture<Response> writeAsync(long messageId, Object msg) {
        String futureKey = configuration.createFutureKey(messageId);
        MessageFuture<Response> future = MessageFuture.createInstance(futureMap, futureKey);
        configuration.getChannelGroup().writeAndFlush(msg);

        return future;
    }
}
