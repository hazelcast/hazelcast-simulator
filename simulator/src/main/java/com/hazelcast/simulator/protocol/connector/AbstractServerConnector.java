package com.hazelcast.simulator.protocol.connector;

import com.hazelcast.simulator.protocol.configuration.ServerConfiguration;
import com.hazelcast.simulator.protocol.core.Response;
import com.hazelcast.simulator.protocol.core.ResponseFuture;
import com.hazelcast.simulator.protocol.core.ResponseType;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.core.SimulatorMessage;
import com.hazelcast.simulator.protocol.core.SimulatorProtocolException;
import com.hazelcast.simulator.protocol.operation.SimulatorOperation;
import com.hazelcast.util.EmptyStatement;
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
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.simulator.protocol.configuration.ServerConfiguration.DEFAULT_SHUTDOWN_QUIET_PERIOD;
import static com.hazelcast.simulator.protocol.configuration.ServerConfiguration.DEFAULT_SHUTDOWN_TIMEOUT;
import static com.hazelcast.simulator.protocol.core.ResponseFuture.createFutureKey;
import static com.hazelcast.simulator.protocol.core.ResponseFuture.createInstance;
import static com.hazelcast.simulator.protocol.operation.OperationCodec.toJson;
import static com.hazelcast.simulator.protocol.operation.OperationType.getOperationType;
import static com.hazelcast.simulator.utils.CommonUtils.sleepMillis;
import static java.lang.String.format;

/**
 * Abstract {@link ServerConnector} class for Simulator Agent and Worker.
 */
abstract class AbstractServerConnector implements ServerConnector {

    private static final Logger LOGGER = Logger.getLogger(AbstractServerConnector.class);

    private static final SimulatorMessage POISON_PILL = new SimulatorMessage(null, null, 0, null, null);

    private final EventLoopGroup bossGroup = new NioEventLoopGroup();
    private final EventLoopGroup workerGroup = new NioEventLoopGroup();

    private final AtomicLong messageIds = new AtomicLong();
    private final BlockingQueue<SimulatorMessage> messageQueue = new LinkedBlockingQueue<SimulatorMessage>();
    private final MessageQueueThread messageQueueThread = new MessageQueueThread();

    private final ServerConfiguration configuration;
    private final ConcurrentMap<String, ResponseFuture> futureMap;
    private final SimulatorAddress localAddress;

    private Channel channel;

    AbstractServerConnector(ServerConfiguration configuration) {
        this.configuration = configuration;
        this.futureMap = configuration.getFutureMap();
        this.localAddress = configuration.getLocalAddress();
    }

    @Override
    public void start() {
        messageQueueThread.start();

        ServerBootstrap bootstrap = getServerBootstrap();
        ChannelFuture future = bootstrap.bind().syncUninterruptibly();
        channel = future.channel();

        LOGGER.info(format("ServerConnector %s listens on %s", configuration.getLocalAddress(), channel.localAddress()));
    }

    private ServerBootstrap getServerBootstrap() {
        ServerBootstrap bootstrap = new ServerBootstrap();
        bootstrap.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .localAddress(new InetSocketAddress(configuration.getLocalPort()))
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel channel) {
                        configuration.configurePipeline(channel.pipeline(), AbstractServerConnector.this);
                    }
                });
        return bootstrap;
    }

    @Override
    public void shutdown() {
        messageQueueThread.shutdown();
        configuration.shutdown();
        channel.close().syncUninterruptibly();

        workerGroup.
                shutdownGracefully(DEFAULT_SHUTDOWN_QUIET_PERIOD, DEFAULT_SHUTDOWN_TIMEOUT, TimeUnit.SECONDS)
                .syncUninterruptibly();
        bossGroup
                .shutdownGracefully(DEFAULT_SHUTDOWN_QUIET_PERIOD, DEFAULT_SHUTDOWN_TIMEOUT, TimeUnit.SECONDS)
                .syncUninterruptibly();
    }

    @Override
    public SimulatorAddress getAddress() {
        return configuration.getLocalAddress();
    }

    @Override
    public ServerConfiguration getConfiguration() {
        return configuration;
    }

    @Override
    public void submit(SimulatorAddress destination, SimulatorOperation operation) {
        SimulatorMessage message = createSimulatorMessage(localAddress, destination, operation);
        messageQueue.add(message);
    }

    @Override
    public Response write(SimulatorAddress destination, SimulatorOperation operation) {
        return write(localAddress, destination, operation);
    }

    @Override
    public Response write(SimulatorAddress source, SimulatorAddress destination, SimulatorOperation operation) {
        try {
            return writeAsync(source, destination, operation).get();
        } catch (InterruptedException e) {
            throw new SimulatorProtocolException("ResponseFuture.get() got interrupted!", e);
        }
    }

    @Override
    public ResponseFuture writeAsync(SimulatorAddress destination, SimulatorOperation operation) {
        return writeAsync(localAddress, destination, operation);
    }

    @Override
    public ResponseFuture writeAsync(SimulatorAddress source, SimulatorAddress destination, SimulatorOperation operation) {
        SimulatorMessage message = createSimulatorMessage(source, destination, operation);
        return writeAsync(message);
    }

    private SimulatorMessage createSimulatorMessage(SimulatorAddress src, SimulatorAddress dst, SimulatorOperation op) {
        return new SimulatorMessage(dst, src, messageIds.incrementAndGet(), getOperationType(op), toJson(op));
    }

    private ResponseFuture writeAsync(SimulatorMessage message) {
        long messageId = message.getMessageId();
        String futureKey = createFutureKey(message.getSource(), messageId, configuration.getLocalAddressIndex());
        ResponseFuture future = createInstance(futureMap, futureKey);
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("[%d] %s created ResponseFuture %s", messageId, configuration.getLocalAddress(), futureKey));
        }
        configuration.getChannelGroup().writeAndFlush(message);

        return future;
    }

    private final class MessageQueueThread extends Thread {

        private static final int WAIT_FOR_EMPTY_QUEUE_MILLIS = 100;

        private MessageQueueThread() {
            super("ServerConnectorMessageQueueThread");
        }

        @Override
        public void run() {
            while (true) {
                try {
                    SimulatorMessage message = messageQueue.take();
                    if (POISON_PILL.equals(message)) {
                        break;
                    }

                    Response response = writeAsync(message).get();
                    for (Map.Entry<SimulatorAddress, ResponseType> entry : response.entrySet()) {
                        if (!entry.getValue().equals(ResponseType.SUCCESS)) {
                            LOGGER.error("Got " + entry.getValue() + " on " + entry.getKey() + " for " + message);
                        }
                    }
                } catch (Exception e) {
                    LOGGER.error("Error while sending message from messageQueue", e);
                    throw new SimulatorProtocolException("Error while sending message from messageQueue", e);
                }
            }
            if (!messageQueue.isEmpty()) {
                LOGGER.error("messageQueue not empty after poison pill was processed: " + messageQueue.peek());
            }
        }

        public void shutdown() {
            try {
                messageQueue.add(POISON_PILL);

                int queueSize = messageQueue.size();
                while (queueSize > 0) {
                    SimulatorMessage message = messageQueue.peek();
                    if (!POISON_PILL.equals(message)) {
                        LOGGER.info(format("%d messages pending on messageQueue, first message: %s", queueSize, message));
                    }
                    sleepMillis(WAIT_FOR_EMPTY_QUEUE_MILLIS);
                    queueSize = messageQueue.size();
                }

                messageQueueThread.join();
            } catch (InterruptedException e) {
                EmptyStatement.ignore(e);
            }
        }
    }
}
