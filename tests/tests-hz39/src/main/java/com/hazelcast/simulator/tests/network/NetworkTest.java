/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.simulator.tests.network;

import com.hazelcast.core.ILock;
import com.hazelcast.core.Member;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.networking.nio.NioEventLoopGroup;
import com.hazelcast.internal.networking.nio.SelectorMode;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.Packet;
import com.hazelcast.nio.tcp.TcpIpConnectionManager;
import com.hazelcast.simulator.test.AbstractTest;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.TestException;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.tests.helpers.HazelcastTestUtils;
import com.hazelcast.spi.impl.PacketHandler;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.nio.Packet.FLAG_OP_RESPONSE;
import static com.hazelcast.simulator.tests.network.NetworkTest.IOThreadingModelEnum.NonBlocking;
import static com.hazelcast.simulator.tests.network.PayloadUtils.addHeadTailMarkers;
import static com.hazelcast.simulator.tests.network.PayloadUtils.checkHeadTailMarkers;
import static com.hazelcast.simulator.tests.network.PayloadUtils.makePayload;
import static com.hazelcast.simulator.tests.network.PayloadUtils.readLong;
import static com.hazelcast.simulator.utils.CommonUtils.sleepMillis;

@SuppressWarnings("checkstyle:npathcomplexity")
public class NetworkTest extends AbstractTest {

    private static final int PORT_OFFSET = 1000;

    public int payloadSize = 0;
    public long requestTimeout = 60;
    public TimeUnit requestTimeUnit = TimeUnit.SECONDS;
    public int inputThreadCount = 1;
    public int outputThreadCount = 1;
    public SelectorMode selectorMode = SelectorMode.SELECT;
    public boolean socketNoDelay = true;
    public int socketReceiveBufferSize = 32;
    public int socketSendBufferSize = 32;
    public IOThreadingModelEnum ioThreadingModel = NonBlocking;
    public boolean trackSequenceId = false;
    public boolean returnPayload = true;

    private final AtomicInteger workerIdGenerator = new AtomicInteger();
    private ILock networkCreateLock;
    private TcpIpConnectionManager connectionManager;
    private RequestPacketHandler packetHandler;
    private List<Connection> connections = new LinkedList<Connection>();

    public enum IOThreadingModelEnum {
        NonBlocking,
        Spinning
    }

    @Setup
    public void setup() throws Exception {
        Node node = HazelcastTestUtils.getNode(targetInstance);
        if (node == null) {
            throw new IllegalStateException("node is null");
        }
        MetricsRegistry metricsRegistry = node.nodeEngine.getMetricsRegistry();
        LoggingService loggingService = node.loggingService;

        // we don't know the number of worker threads (damn hidden property), so lets assume 1000.. that should be enough
        packetHandler = new RequestPacketHandler(1000);

        Address thisAddress = node.getThisAddress();
        Address newThisAddress = new Address(thisAddress.getHost(), thisAddress.getPort() + PORT_OFFSET);
        logger.info("ThisAddress: " + newThisAddress);
        MockIOService ioService = new MockIOService(newThisAddress, loggingService);
        ioService.inputThreadCount = inputThreadCount;
        ioService.outputThreadCount = outputThreadCount;
        ioService.socketNoDelay = socketNoDelay;
        ioService.packetHandler = packetHandler;
        ioService.socketSendBufferSize = socketSendBufferSize;
        ioService.socketReceiveBufferSize = socketReceiveBufferSize;

        if (trackSequenceId) {
            ioService.writeHandlerFactory = new TaggingWriteHandlerFactory();
        }

        NioEventLoopGroup threadingModel = null;
        switch (ioThreadingModel) {
//            case NonBlocking:
//                NonBlockingIOThreadingModel nonBlockingIOThreadingModel = new NonBlockingIOThreadingModel(
//                        ioService, loggingService, metricsRegistry, threadGroup, null, null, null);
//                selectorMode = SelectorMode.SELECT;
//                nonBlockingIOThreadingModel.setSelectorMode(selectorMode);
//                threadingModel = nonBlockingIOThreadingModel;
//                break;
//            case Spinning:
//                threadingModel = new SpinningIOThreadingModel(loggingService, threadGroup);
//                break;
            default:
                throw new IllegalStateException("Unrecognized threading model: " + ioThreadingModel);
        }
//
//        connectionManager = new TcpIpConnectionManager(
//                ioService, ioService.serverSocketChannel, loggingService, metricsRegistry, threadingModel);
//        connectionManager.start();
//        networkCreateLock = targetInstance.getLock("connectionCreateLock");
    }

    @Prepare
    public void prepare() throws Exception {
        networkCreateLock.lock();
        try {
            logger.info("Starting connections: " + (targetInstance.getCluster().getMembers().size() - 1));
            for (Member member : targetInstance.getCluster().getMembers()) {

                if (member.localMember()) {
                    continue;
                }

                Address memberAddress = member.getAddress();
                Address targetAddress = new Address(memberAddress.getHost(), memberAddress.getPort() + PORT_OFFSET);

                logger.info("Connecting to: " + targetAddress);
                connectionManager.getOrConnect(targetAddress);
                Connection connection;
                for (; ; ) {
                    connection = connectionManager.getConnection(targetAddress);
                    if (connection != null) {
                        break;
                    }

                    logger.info("Waiting for connection to: " + targetAddress);
                    sleepMillis(100);
                }

                connections.add(connection);

                logger.info("Successfully created connection to: " + targetAddress);
            }

            logger.info("Successfully started all connections");
        } finally {
            networkCreateLock.unlock();
        }

        // temporary delay
        Thread.sleep(30);
    }


    @TimeStep
    public void timeStep(ThreadState state) throws Exception {
        if (state.responseFuture.thread == null) {
            state.responseFuture.thread = Thread.currentThread();
        }

        Connection connection = state.nextConnection();
        byte[] payload = makePayload(payloadSize);
        Packet requestPacket = new Packet(payload, state.workerId);

        if (!connection.write(requestPacket)) {
            throw new TestException("Failed to write packet to connection %s", connection);
        }

        try {
            state.responseFuture.get(requestTimeout, requestTimeUnit);
        } catch (Exception e) {
            throw new TestException("Failed to receive request from connection %s within timeout %d %s", connection,
                    requestTimeout, requestTimeUnit, e);
        }
        state.responseFuture.reset();
    }

    public class ThreadState extends BaseThreadState {

        private final int workerId;
        private final RequestFuture responseFuture;

        public ThreadState() {
            workerId = workerIdGenerator.getAndIncrement();
            responseFuture = packetHandler.futures[workerId];

        }

        private Connection nextConnection() {
            int index = randomInt(connections.size());
            return connections.get(index);
        }
    }

    private class RequestPacketHandler implements PacketHandler {

        private final RequestFuture[] futures;
        private final ConcurrentHashMap<Connection, AtomicLong> sequenceCounterMap
                = new ConcurrentHashMap<Connection, AtomicLong>();

        RequestPacketHandler(int threadCount) {
            futures = new RequestFuture[threadCount];
            for (int i = 0; i < futures.length; i++) {
                futures[i] = new RequestFuture();
            }
        }

        @Override
        public void handle(Packet packet) throws Exception {
            checkPayloadSize(packet);
            checkPayloadContent(packet);

            if (packet.isFlagRaised(FLAG_OP_RESPONSE)) {
                handleResponse(packet);
            } else {
                handleRequest(packet);
            }
        }

        private void handleRequest(Packet packet) {
            // it is a request, then we send back a response
            byte[] requestPayload = packet.toByteArray();
            byte[] responsePayload = null;
            if (requestPayload != null && returnPayload) {
                responsePayload = new byte[requestPayload.length];
                addHeadTailMarkers(responsePayload);
            }
            Packet response = new Packet(responsePayload, packet.getPartitionId());
            response.setPacketType(Packet.Type.OPERATION).raiseFlags(FLAG_OP_RESPONSE);
            packet.getConn().write(response);
        }

        private void handleResponse(Packet packet) {
            RequestFuture future = futures[packet.getPartitionId()];
            future.set();
        }

        private void checkPayloadContent(Packet packet) {
            byte[] payload = packet.toByteArray();
            int foundPayloadSize = payload == null ? 0 : payload.length;

            if (foundPayloadSize <= 0) {
                return;
            }

            checkHeadTailMarkers(payload);

            if (!trackSequenceId) {
                return;
            }

            AtomicLong sequenceCounter = sequenceCounterMap.get(packet.getConn());
            if (sequenceCounter == null) {
                AtomicLong newSequenceCounter = new AtomicLong(0);
                sequenceCounter = sequenceCounterMap.putIfAbsent(packet.getConn(), newSequenceCounter);
                if (sequenceCounter == null) {
                    sequenceCounter = newSequenceCounter;
                }
            }

            long foundSequence = readLong(payload, 3);
            long expectedSequence = sequenceCounter.get() + 1;
            if (expectedSequence != foundSequence) {
                throw new IllegalArgumentException("Unexpected sequence id, expected: " + expectedSequence
                        + "found: " + foundSequence);
            }
            sequenceCounter.set(expectedSequence);
        }

        private void checkPayloadSize(Packet packet) {
            byte[] payload = packet.toByteArray();
            int foundPayloadSize = payload == null ? 0 : payload.length;
            int expectedPayloadSize;

            if (packet.isFlagRaised(FLAG_OP_RESPONSE) && !returnPayload) {
                expectedPayloadSize = 0;
            } else {
                expectedPayloadSize = payloadSize;
            }

            if (foundPayloadSize != expectedPayloadSize) {
                throw new IllegalArgumentException("Unexpected payload size; expected: " + expectedPayloadSize
                        + " but found: " + foundPayloadSize);
            }
        }
    }

    @Teardown
    public void teardown() {
        connectionManager.shutdown();
    }
}
