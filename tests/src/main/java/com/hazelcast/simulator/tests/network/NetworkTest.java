package com.hazelcast.simulator.tests.network;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ILock;
import com.hazelcast.core.Member;
import com.hazelcast.instance.HazelcastThreadGroup;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.Packet;
import com.hazelcast.nio.tcp.IOThreadingModel;
import com.hazelcast.nio.tcp.TcpIpConnection;
import com.hazelcast.nio.tcp.TcpIpConnectionManager;
import com.hazelcast.nio.tcp.nonblocking.NonBlockingIOThreadingModel;
import com.hazelcast.nio.tcp.spinning.SpinningIOThreadingModel;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.TestRunner;
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.test.annotations.Warmup;
import com.hazelcast.simulator.tests.helpers.HazelcastTestUtils;
import com.hazelcast.simulator.worker.tasks.AbstractMonotonicWorker;
import com.hazelcast.spi.impl.PacketHandler;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.hazelcast.simulator.tests.network.NetworkTest.IOThreadingModelEnum.NonBlocking;
import static com.hazelcast.simulator.utils.CommonUtils.sleepMillis;


public class NetworkTest {

    private static final int PORT_OFFSET = 1000;

    private static final ILogger LOGGER = Logger.getLogger(NetworkTest.class);

    public int payloadSize = 0;
    public long requestTimeout = 60;
    public TimeUnit requestTimeUnit = TimeUnit.SECONDS;
    public int inputThreadCount = 1;
    public int outputThreadCount = 1;
    public boolean inputSelectNow = false;
    public boolean outputSelectNow = false;
    public boolean socketNoDelay = true;
    public int socketReceiveBufferSize = 32;
    public int socketSendBufferSize = 32;
    public IOThreadingModelEnum ioThreadingModel = NonBlocking;

    private final AtomicInteger workerIdGenerator = new AtomicInteger();
    private HazelcastInstance hz;
    private ILock networkCreateLock;
    private TcpIpConnectionManager connectionManager;
    private DummyPacketHandler packetHandler;

    private ConcurrentHashMap<Connection, AtomicLong> counters = new ConcurrentHashMap<Connection, AtomicLong>();

    public enum IOThreadingModelEnum {
        NonBlocking,
        Spinning
    }

    @Setup
    public void setup(TestContext context) throws Exception {
        hz = context.getTargetInstance();

        Node node = HazelcastTestUtils.getNode(hz);
        if (node == null) {
            throw new IllegalStateException("node is null");
        }
        MetricsRegistry metricsRegistry = node.nodeEngine.getMetricsRegistry();
        LoggingService loggingService = node.loggingService;
        HazelcastThreadGroup threadGroup = node.getHazelcastThreadGroup();

        // we don't know the number of worker threads (damn hidden property), so lets assume 1000.. that should be enough
        packetHandler = new DummyPacketHandler(1000);

        Address thisAddress = node.getThisAddress();
        Address newThisAddress = new Address(thisAddress.getHost(), thisAddress.getPort() + PORT_OFFSET);
        LOGGER.info("ThisAddress: " + newThisAddress);
        MockIOService ioService = new MockIOService(newThisAddress, loggingService);
        ioService.inputThreadCount = inputThreadCount;
        ioService.outputThreadCount = outputThreadCount;
        ioService.socketNoDelay = socketNoDelay;
        ioService.packetHandler = packetHandler;
        ioService.socketSendBufferSize = socketSendBufferSize;
        ioService.socketReceiveBufferSize = socketReceiveBufferSize;

        IOThreadingModel threadingModel;
        switch (ioThreadingModel) {
            case NonBlocking:
                NonBlockingIOThreadingModel nonBlockingIOThreadingModel = new NonBlockingIOThreadingModel(
                        ioService, loggingService, metricsRegistry, threadGroup);
                nonBlockingIOThreadingModel.setInputSelectNow(inputSelectNow);
                nonBlockingIOThreadingModel.setOutputSelectNow(outputSelectNow);
                threadingModel = nonBlockingIOThreadingModel;
                break;
            case Spinning:
                threadingModel = new SpinningIOThreadingModel(ioService, loggingService, metricsRegistry, threadGroup);
                break;
            default:
                throw new IllegalStateException("Unrecognized threading model:" + ioThreadingModel);
        }

        connectionManager = new TcpIpConnectionManager(
                ioService, ioService.serverSocketChannel, loggingService, metricsRegistry, threadingModel);
        connectionManager.start();
        networkCreateLock = hz.getLock("connectionCreateLock");
    }

    @Teardown
    public void teardown() throws Exception {
        connectionManager.shutdown();
    }

    @Warmup
    public void warmup() throws Exception {
        networkCreateLock.lock();
        try {
            LOGGER.info("Starting connections: " + (hz.getCluster().getMembers().size() - 1));
            for (Member member : hz.getCluster().getMembers()) {

                if (member.localMember()) {
                    continue;
                }

                Address memberAddress = member.getAddress();
                Address targetAddress = new Address(memberAddress.getHost(), memberAddress.getPort() + PORT_OFFSET);

                LOGGER.info("Connecting to: " + targetAddress);

                connectionManager.getOrConnect(targetAddress);

                while (connectionManager.getConnection(targetAddress) == null) {
                    LOGGER.info("Waiting for connection to: " + targetAddress);
                    sleepMillis(100);
                }

                counters.put(connectionManager.getConnection(targetAddress), new AtomicLong());

                LOGGER.info("Successfully created connection to: " + targetAddress);
            }

            LOGGER.info("Successfully started all connections");
        } finally {
            networkCreateLock.unlock();
        }
    }

    @Verify
    public void verify() {
    }

    @RunWithWorker
    public Worker createWorker() {
        return new Worker();
    }

    private class Worker extends AbstractMonotonicWorker {

        private final int workerId;
        private final RequestFuture responseFuture;
        private final List<TcpIpConnection> connections;

        public Worker() {
            workerId = workerIdGenerator.getAndIncrement();
            responseFuture = packetHandler.futures[workerId];
            connections = new ArrayList<TcpIpConnection>(connectionManager.getActiveConnections());
        }

        private byte[] makePayload(final long sequenceId) {
            byte[] payload = null;
            if (payloadSize > 0) {
                payload = new byte[payloadSize];
                //getRandom().nextBytes(payload);

                // put a well known head and tail on the payload; for debugging.
                if (payload.length >= 6 + 8) {
                    payload[0] = 0xA;
                    payload[1] = 0xB;
                    payload[2] = 0xC;

                    // we also stuff in a sequence id at the beginning
                    long s = sequenceId;
                    for (int i = 7; i >= 0; i--) {
                        payload[i + 3] = (byte) (s & 0xFF);
                        s >>= 8;
                    }

                    // and a sequence id at the end.
                    s = sequenceId;
                    for (int i = 7; i >= 0; i--) {
                        payload[i + payload.length - (8 + 3)] = (byte) (s & 0xFF);
                        s >>= 8;
                    }

                    payload[payload.length - 3] = 0xC;
                    payload[payload.length - 2] = 0xB;
                    payload[payload.length - 1] = 0xA;
                }
            }
            return payload;
        }

        @Override
        protected void timeStep() {
            TcpIpConnection connection = nextConnection();
            AtomicLong counter = counters.get(connection);
            synchronized (connection) {
                long count = counter.incrementAndGet();
                byte[] payload = makePayload(count);
                Packet packet = new Packet(payload, workerId);
                boolean success = connection.write(packet);
                if (!success) {
                    throw new RuntimeException("Failed to write packet to connection:" + connection);
                }
            }

            try {
                responseFuture.get(requestTimeout, requestTimeUnit);
            } catch (Exception e) {
                throw new RuntimeException("Failed to receive request from connection:"
                        + connection + " within timeout:" + requestTimeout + " " + requestTimeUnit);
            }
            responseFuture.reset();
        }

        private TcpIpConnection nextConnection() {
            int index = randomInt(connections.size());
            return connections.get(index);
        }
    }

    private class DummyPacketHandler implements PacketHandler {

        private final RequestFuture[] futures;
        private final ConcurrentHashMap<Connection,AtomicLong> sequenceMap = new ConcurrentHashMap<Connection, AtomicLong>();

        public DummyPacketHandler(int threadCount) {
            futures = new RequestFuture[threadCount];
            for (int k = 0; k < futures.length; k++) {
                futures[k] = new RequestFuture();
            }
        }

        @Override
        public void handle(Packet packet) throws Exception {
            byte[] data = packet.toByteArray();
            int foundPayloadSize = data == null ? 0 : data.length;

            if (foundPayloadSize > 0) {
                byte[] payload = packet.toByteArray();
                check(payload, 0, 0XA);
                check(payload, 1, 0XB);
                check(payload, 2, 0XC);

                AtomicLong sequenceCounter = sequenceMap.get(packet.getConn());
                if(sequenceCounter==null){
                    sequenceCounter = new AtomicLong();
                    counters.put(packet.getConn(), sequenceCounter);
                }

                long sequence = bytesToLong(payload, 3);
                if (sequenceCounter.get() + 1 != sequence) {
                    throw new IllegalArgumentException("Unexpected sequence id, expected:" + (sequenceCounter.get() + 1)
                            + "found:" + sequence);
                }
                sequenceCounter.incrementAndGet();

                check(payload, payload.length - 3, 0XC);
                check(payload, payload.length - 2, 0XB);
                check(payload, payload.length - 1, 0XA);
            }

            if (foundPayloadSize != payloadSize) {
                throw new IllegalArgumentException("Unexpected payload size; expected:" + payloadSize
                        + " but found:" + foundPayloadSize);
            }

            if (packet.isHeaderSet(Packet.HEADER_RESPONSE)) {
                futures[packet.getPartitionId()].set();
            } else {
                Packet response = new Packet(packet.toByteArray(), packet.getPartitionId());
                response.setHeader(Packet.HEADER_RESPONSE);
                packet.getConn().write(response);
            }
        }

        public long bytesToLong(byte[] bytes, int offset) {
            long result = 0;
            for (int i = 0; i < 8; i++) {
                result <<= 8;
                result |= (bytes[i + offset] & 0xFF);
            }
            return result;
        }

        private void check(byte[] payload, int index, int value) {
            if (payload[index] != value) {
                throw new IllegalStateException();
            }
        }
    }

    public static class RequestFuture implements Future {

        private final Lock lock = new ReentrantLock(false);
        private final Condition condition = lock.newCondition();
        private volatile Object result = null;

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isCancelled() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isDone() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object get() throws InterruptedException, ExecutionException {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            if (result != null) {
                return result;
            }

            long timeoutNs = unit.toNanos(timeout);
            lock.lock();
            try {
                while (result == null) {
                    if (timeoutNs <= 0) {
                        throw new TimeoutException();
                    }

                    timeoutNs = condition.awaitNanos(timeoutNs);
                }

                return result;
            } finally {
                lock.unlock();
            }
        }

        public void set() {
            lock.lock();
            try {
                if (result != null) {
                    throw new RuntimeException("Result should be null");
                }
                result = Boolean.TRUE;
                condition.signal();
            } finally {
                lock.unlock();
            }
        }

        public void reset() {
            if (result == null) {
                throw new RuntimeException("result can't be null");
            }
            result = null;
        }
    }

    public static void main(String[] args) throws Exception {
        NetworkTest test = new NetworkTest();
        new TestRunner<NetworkTest>(test).withDuration(10).run();
    }
}
