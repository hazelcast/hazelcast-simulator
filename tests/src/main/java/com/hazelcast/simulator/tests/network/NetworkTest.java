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
import com.hazelcast.nio.Packet;
import com.hazelcast.nio.tcp.TcpIpConnection;
import com.hazelcast.nio.tcp.TcpIpConnectionManager;
import com.hazelcast.nio.tcp.TcpIpConnectionThreadingModel;
import com.hazelcast.nio.tcp.nonblocking.NonBlockingTcpIpConnectionThreadingModel;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.TestRunner;
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.test.annotations.Warmup;
import com.hazelcast.simulator.tests.concurrent.atomiclong.AtomicLongTest;
import com.hazelcast.simulator.tests.helpers.HazelcastTestUtils;
import com.hazelcast.simulator.worker.tasks.AbstractMonotonicWorker;
import com.hazelcast.spi.impl.PacketHandler;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


public class NetworkTest {

    private static final ILogger LOGGER = Logger.getLogger(NetworkTest.class);

    private static final int PORT_OFFSET = 1000;

    public int payloadSize = 0;
    public long requestTimeout = 60;
    public TimeUnit requestTimeUnit = TimeUnit.SECONDS;
    public int inputThreadCount;
    public int outputThreadCount;

    private HazelcastInstance hz;
    private ILock networkCreateLock;
    private TcpIpConnectionManager connectionManager;
    private MockIOService ioService;
    private DummyPacketHandler packetHandler;
    private AtomicInteger workerIdGenerator = new AtomicInteger();
    private TcpIpConnectionThreadingModel threadingModel;


    @Setup
    public void setup(TestContext context) throws Exception {
        hz = context.getTargetInstance();

        // we don't know the number of worker threads (damn hidden property), so lets assume 1000.. that should be enough
        packetHandler = new DummyPacketHandler(1000);

        int serverPort = hz.getCluster().getLocalMember().getSocketAddress().getPort();
        ioService = new MockIOService(serverPort + PORT_OFFSET);
        ioService.inputThreadCount = inputThreadCount;
        ioService.outputThreadCount = outputThreadCount;

        Node node = HazelcastTestUtils.getNode(hz);
        MetricsRegistry metricsRegistry = node.nodeEngine.getMetricsRegistry();
        LoggingService loggingService = node.loggingService;
        HazelcastThreadGroup threadGroup = node.getHazelcastThreadGroup();

        threadingModel = new NonBlockingTcpIpConnectionThreadingModel(ioService, loggingService, metricsRegistry, threadGroup);

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
            for (Member member : hz.getCluster().getMembers()) {
                if (member.localMember()) {
                    continue;
                }

                Address targetAddress = member.getAddress();
                Address newAddress = new Address(targetAddress.getHost(), targetAddress.getPort() + PORT_OFFSET);
                connectionManager.getOrConnect(newAddress);

                for (; ; ) {
                    if (connectionManager.getConnection(newAddress) != null) {
                        break;
                    }
                    Thread.sleep(100);
                }
            }
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
        private final RequestFuture future;
        private final List<TcpIpConnection> connections;
        private byte[] payload;

        public Worker() {
            workerId = workerIdGenerator.getAndIncrement();
            if (payloadSize > 0) {
                payload = new byte[payloadSize];
                getRandom().nextBytes(payload);
            }
            future = packetHandler.futures[workerId];
            connections = new ArrayList<TcpIpConnection>(connectionManager.getActiveConnections());
        }

        @Override
        protected void timeStep() {
            Packet packet = new Packet(payload, workerId);
            TcpIpConnection connection = nextConnection();
            boolean success = connection.write(packet);
            if (!success) {
                throw new RuntimeException("Failed to write packet to connection:" + connection);
            }

            try {
                future.get(requestTimeout, requestTimeUnit);
            } catch (Exception e) {
                throw new RuntimeException("Failed to receive request from connection:"
                        + connection + " within timeout:" + requestTimeout + " " + requestTimeUnit);
            }
            future.reset();
        }

        private TcpIpConnection nextConnection() {
            int index = randomInt(connections.size());
            return connections.get(index);
        }
    }

    private class DummyPacketHandler implements PacketHandler {

        private final RequestFuture[] futures;

        public DummyPacketHandler(int threadCount) {
            futures = new RequestFuture[threadCount];
            for (int k = 0; k < futures.length; k++) {
                futures[k] = new RequestFuture();
            }
        }

        @Override
        public void handle(Packet packet) throws Exception {
            if (packet.isHeaderSet(Packet.HEADER_RESPONSE)) {
                futures[packet.getPartitionId()].set();
            } else {
                Packet response = new Packet(packet.toByteArray(), packet.getPartitionId());
                response.setHeader(Packet.HEADER_RESPONSE);
                packet.getConn().write(response);
            }
        }
    }

    public class RequestFuture implements Future {

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
                while (result != null) {
                    if (timeout <= 0) {
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
                result = Boolean.TRUE;
                condition.signal();
            } finally {
                lock.unlock();
            }
        }

        public void reset() {
            result = null;
        }
    }

    public static void main(String[] args) throws Exception {
        AtomicLongTest test = new AtomicLongTest();
        new TestRunner<AtomicLongTest>(test).withDuration(10).run();
    }


}
