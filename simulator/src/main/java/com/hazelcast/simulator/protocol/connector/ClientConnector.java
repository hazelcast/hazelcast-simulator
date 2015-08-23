package com.hazelcast.simulator.protocol.connector;

import com.hazelcast.simulator.protocol.configuration.ClientConfiguration;
import com.hazelcast.simulator.protocol.core.MessageFuture;
import com.hazelcast.simulator.protocol.core.Response;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.simulator.protocol.configuration.ServerConfiguration.DEFAULT_SHUTDOWN_QUIET_PERIOD;
import static com.hazelcast.simulator.protocol.configuration.ServerConfiguration.DEFAULT_SHUTDOWN_TIMEOUT;
import static com.hazelcast.simulator.protocol.core.SimulatorMessageCodec.getMessageId;
import static java.lang.String.format;

/**
 * Client connector for a Simulator Coordinator or Agent.
 */
public class ClientConnector {

    private static final Logger LOGGER = Logger.getLogger(ClientConnector.class);

    private final EventLoopGroup group = new NioEventLoopGroup();

    private final ConcurrentMap<String, MessageFuture<Response>> futureMap
            = new ConcurrentHashMap<String, MessageFuture<Response>>();

    private final ClientConfiguration configuration;

    private Channel channel;

    public ClientConnector(ClientConfiguration configuration) {
        this.configuration = configuration;
    }

    public void start() {
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(group)
                .channel(NioSocketChannel.class)
                .remoteAddress(new InetSocketAddress(configuration.getHost(), configuration.getPort()))
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel channel) {
                        configuration.configurePipeline(channel.pipeline(), futureMap);
                    }
                });

        ChannelFuture future = bootstrap.connect().syncUninterruptibly();
        channel = future.channel();

        LOGGER.info(format("%s started and sends to %s", ClientConnector.class.getName(), channel.remoteAddress()));
    }

    public void shutdown() {
        group.shutdownGracefully(DEFAULT_SHUTDOWN_QUIET_PERIOD, DEFAULT_SHUTDOWN_TIMEOUT, TimeUnit.SECONDS).syncUninterruptibly();
    }

    public Response write(SimulatorMessage message) throws Exception {
        return writeAsync(message).get();
    }

    public Response write(ByteBuf buffer) throws Exception {
        return writeAsync(buffer).get();
    }

    private MessageFuture<Response> writeAsync(SimulatorMessage message) {
        return writeAsync(message.getMessageId(), message);
    }

    private MessageFuture<Response> writeAsync(ByteBuf buffer) {
        return writeAsync(getMessageId(buffer), buffer);
    }

    private MessageFuture<Response> writeAsync(long messageId, Object msg) {
        String futureKey = configuration.createFutureKey(messageId);
        MessageFuture<Response> future = MessageFuture.createInstance(futureMap, futureKey);
        channel.writeAndFlush(msg);

        return future;
    }
}
