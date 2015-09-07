package com.hazelcast.simulator.protocol.connector;

import com.hazelcast.simulator.protocol.configuration.ServerConfiguration;
import com.hazelcast.simulator.protocol.core.Response;
import com.hazelcast.simulator.protocol.core.ResponseFuture;
import com.hazelcast.simulator.protocol.core.ResponseType;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.core.SimulatorMessage;
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
import static com.hazelcast.simulator.protocol.operation.OperationHandler.encodeOperation;
import static com.hazelcast.simulator.protocol.operation.OperationType.getOperationType;
import static com.hazelcast.simulator.utils.CommonUtils.sleepMillis;
import static java.lang.String.format;

/**
 * Abstract {@link ServerConnector} class for Simulator Agent and Worker.
 */
abstract class AbstractServerConnector implements ServerConnector {

    private static final Logger LOGGER = Logger.getLogger(AbstractServerConnector.class);

    private final EventLoopGroup group = new NioEventLoopGroup();

    private final AtomicLong messageIds = new AtomicLong();
    private final BlockingQueue<SimulatorMessage> messageQueue = new LinkedBlockingQueue<SimulatorMessage>();
    private final MessageQueueThread messageQueueThread = new MessageQueueThread();

    private final ServerConfiguration configuration;
    private final ConcurrentMap<String, ResponseFuture> futureMap;
    private final SimulatorAddress localAddress;

    AbstractServerConnector(ServerConfiguration configuration) {
        this.configuration = configuration;
        this.futureMap = configuration.getFutureMap();
        this.localAddress = configuration.getLocalAddress();
    }

    @Override
    public void start() {
        messageQueueThread.start();

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

    @Override
    public void shutdown() {
        messageQueueThread.shutdown();

        group.shutdownGracefully(DEFAULT_SHUTDOWN_QUIET_PERIOD, DEFAULT_SHUTDOWN_TIMEOUT, TimeUnit.SECONDS).syncUninterruptibly();
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
    public Response write(SimulatorAddress destination, SimulatorOperation operation) throws Exception {
        SimulatorMessage message = createSimulatorMessage(localAddress, destination, operation);
        return writeAsync(message).get();
    }

    @Override
    public Response write(SimulatorAddress source, SimulatorAddress destination, SimulatorOperation operation) throws Exception {
        SimulatorMessage message = createSimulatorMessage(source, destination, operation);
        return writeAsync(message).get();
    }

    private SimulatorMessage createSimulatorMessage(SimulatorAddress source, SimulatorAddress destination,
                                                    SimulatorOperation operation) {
        return new SimulatorMessage(destination, source, messageIds.incrementAndGet(),
                getOperationType(operation), encodeOperation(operation));
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

        private volatile boolean running = true;

        private MessageQueueThread() {
            super("ServerConnectorMessageQueueThread");
        }

        @Override
        public void run() {
            while (running) {
                try {
                    SimulatorMessage message = messageQueue.take();
                    Response response = writeAsync(message).get();
                    for (Map.Entry<SimulatorAddress, ResponseType> entry : response.entrySet()) {
                        if (!entry.getValue().equals(ResponseType.SUCCESS)) {
                            LOGGER.error("Got " + entry.getValue() + " on " + entry.getKey() + " for " + message);
                        }
                    }
                } catch (InterruptedException e) {
                    EmptyStatement.ignore(e);
                } catch (Throwable e) {
                    LOGGER.error("Error while sending message from messageQueue", e);
                }
            }
        }

        public void shutdown() {
            try {
                while (messageQueue.size() > 0) {
                    sleepMillis(WAIT_FOR_EMPTY_QUEUE_MILLIS);
                }
                running = false;

                messageQueueThread.interrupt();
                messageQueueThread.join();
            } catch (InterruptedException e) {
                EmptyStatement.ignore(e);
            }
        }
    }
}
