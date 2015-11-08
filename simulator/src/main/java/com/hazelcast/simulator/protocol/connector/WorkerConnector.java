/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.simulator.protocol.connector;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.simulator.protocol.core.ConnectionManager;
import com.hazelcast.simulator.protocol.core.ResponseFuture;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.core.TestProcessorManager;
import com.hazelcast.simulator.protocol.exception.ExceptionLogger;
import com.hazelcast.simulator.protocol.exception.FileExceptionLogger;
import com.hazelcast.simulator.protocol.exception.RemoteExceptionLogger;
import com.hazelcast.simulator.protocol.handler.ConnectionListenerHandler;
import com.hazelcast.simulator.protocol.handler.ConnectionValidationHandler;
import com.hazelcast.simulator.protocol.handler.ExceptionHandler;
import com.hazelcast.simulator.protocol.handler.MessageConsumeHandler;
import com.hazelcast.simulator.protocol.handler.MessageEncoder;
import com.hazelcast.simulator.protocol.handler.MessageTestConsumeHandler;
import com.hazelcast.simulator.protocol.handler.ResponseEncoder;
import com.hazelcast.simulator.protocol.handler.ResponseHandler;
import com.hazelcast.simulator.protocol.handler.SimulatorFrameDecoder;
import com.hazelcast.simulator.protocol.handler.SimulatorProtocolDecoder;
import com.hazelcast.simulator.protocol.operation.SimulatorOperation;
import com.hazelcast.simulator.protocol.processors.OperationProcessor;
import com.hazelcast.simulator.protocol.processors.TestOperationProcessor;
import com.hazelcast.simulator.protocol.processors.WorkerOperationProcessor;
import com.hazelcast.simulator.worker.Worker;
import com.hazelcast.simulator.worker.WorkerType;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.group.ChannelGroup;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.simulator.protocol.core.AddressLevel.WORKER;
import static com.hazelcast.simulator.protocol.exception.ExceptionType.WORKER_EXCEPTION;

/**
 * Connector which listens for incoming Simulator Agent connections and manages Simulator Test instances.
 */
public class WorkerConnector extends AbstractServerConnector {

    private final OperationProcessor processor;

    private final SimulatorAddress localAddress;
    private final int addressIndex;

    private final ConnectionManager connectionManager;
    private final TestProcessorManager testProcessorManager;
    private final ConcurrentMap<String, ResponseFuture> futureMap;

    WorkerConnector(ConcurrentMap<String, ResponseFuture> futureMap, SimulatorAddress localAddress, int port,
                    boolean useRemoteLogger, WorkerType type, HazelcastInstance hazelcastInstance, Worker worker,
                    ConnectionManager connectionManager) {
        super(futureMap, localAddress, port);

        ExceptionLogger exceptionLogger = createExceptionLogger(localAddress, useRemoteLogger);
        this.processor = new WorkerOperationProcessor(exceptionLogger, type, hazelcastInstance, worker, localAddress);

        this.localAddress = localAddress;
        this.addressIndex = localAddress.getAddressIndex();

        this.connectionManager = connectionManager;
        this.testProcessorManager = new TestProcessorManager(localAddress);
        this.futureMap = futureMap;
    }

    @Override
    void configureServerPipeline(ChannelPipeline pipeline, ServerConnector serverConnector) {
        pipeline.addLast("connectionValidationHandler", new ConnectionValidationHandler());
        pipeline.addLast("connectionListenerHandler", new ConnectionListenerHandler(connectionManager));
        pipeline.addLast("responseEncoder", new ResponseEncoder(localAddress));
        pipeline.addLast("messageEncoder", new MessageEncoder(localAddress, localAddress.getParent()));
        pipeline.addLast("frameDecoder", new SimulatorFrameDecoder());
        pipeline.addLast("protocolDecoder", new SimulatorProtocolDecoder(localAddress));
        pipeline.addLast("messageConsumeHandler", new MessageConsumeHandler(localAddress, processor));
        pipeline.addLast("testProtocolDecoder", new SimulatorProtocolDecoder(localAddress.getChild(0)));
        pipeline.addLast("testMessageConsumeHandler", new MessageTestConsumeHandler(testProcessorManager, localAddress));
        pipeline.addLast("responseHandler", new ResponseHandler(localAddress, localAddress.getParent(), futureMap, addressIndex));
        pipeline.addLast("exceptionHandler", new ExceptionHandler(serverConnector));
    }

    @Override
    void connectorShutdown() {
        processor.shutdown();
    }

    @Override
    ChannelGroup getChannelGroup() {
        connectionManager.waitForAtLeastOneChannel();
        return connectionManager.getChannels();
    }

    /**
     * Creates a {@link WorkerConnector} instance.
     *
     * @param parentAddressIndex the index of the parent Simulator Agent
     * @param addressIndex       the index of this Simulator Worker
     * @param port               the port for incoming connections
     * @param type               the {@link WorkerType} of this Simulator Worker
     * @param hazelcastInstance  the {@link HazelcastInstance} for this Simulator Worker
     * @param worker             the {@link Worker} instance of this Simulator Worker
     */
    public static WorkerConnector createInstance(int parentAddressIndex, int addressIndex, int port, WorkerType type,
                                                 HazelcastInstance hazelcastInstance, Worker worker) {
        return createInstance(parentAddressIndex, addressIndex, port, type, hazelcastInstance, worker, false);
    }

    /**
     * Creates a {@link WorkerConnector} instance.
     *
     * @param parentAddressIndex the index of the parent Simulator Agent
     * @param addressIndex       the index of this Simulator Worker
     * @param port               the port for incoming connections
     * @param type               the {@link WorkerType} of this Simulator Worker
     * @param hazelcastInstance  the {@link HazelcastInstance} for this Simulator Worker
     * @param worker             the {@link Worker} instance of this Simulator Worker
     * @param useRemoteLogger    determines if the {@link RemoteExceptionLogger} or {@link FileExceptionLogger} should be used
     */
    public static WorkerConnector createInstance(int parentAddressIndex, int addressIndex, int port, WorkerType type,
                                                 HazelcastInstance hazelcastInstance, Worker worker, boolean useRemoteLogger) {
        ConcurrentMap<String, ResponseFuture> futureMap = new ConcurrentHashMap<String, ResponseFuture>();
        SimulatorAddress localAddress = new SimulatorAddress(WORKER, parentAddressIndex, addressIndex, 0);
        ConnectionManager connectionManager = new ConnectionManager();

        return new WorkerConnector(futureMap, localAddress, port, useRemoteLogger, type, hazelcastInstance, worker,
                connectionManager);
    }

    /**
     * Gets a Simulator Test.
     *
     * @param testIndex the index of the Simulator Test
     * @return the {@link TestOperationProcessor} which processes incoming
     * {@link com.hazelcast.simulator.protocol.operation.SimulatorOperation} for this test
     */
    public TestOperationProcessor getTest(int testIndex) {
        return testProcessorManager.getTest(testIndex);
    }

    /**
     * Adds a Simulator Test.
     *
     * @param testIndex the index of the Simulator Test
     * @param processor the {@link TestOperationProcessor} which processes incoming
     *                  {@link com.hazelcast.simulator.protocol.operation.SimulatorOperation} for this test
     */
    public void addTest(int testIndex, TestOperationProcessor processor) {
        testProcessorManager.addTest(testIndex, processor);
    }

    /**
     * Removes a Simulator Test.
     *
     * @param testIndex the index of the remote Simulator Test
     */
    public void removeTest(int testIndex) {
        testProcessorManager.removeTest(testIndex);
    }

    /**
     * Submits a {@link SimulatorOperation} to a {@link SimulatorAddress}.
     *
     * The {@link SimulatorOperation} is put on a queue. The {@link com.hazelcast.simulator.protocol.core.Response} is not
     * returned.
     *
     * @param testAddress the {@link SimulatorAddress} of the sending test
     * @param destination the {@link SimulatorAddress} of the destination
     * @param operation   the {@link SimulatorOperation} to send
     * @return a {@link ResponseFuture} to wait for the result of the operation
     */
    public ResponseFuture submitFromTest(SimulatorAddress testAddress, SimulatorAddress destination,
                                         SimulatorOperation operation) {
        return submit(testAddress, destination, operation);
    }

    public OperationProcessor getProcessor() {
        return processor;
    }

    private ExceptionLogger createExceptionLogger(SimulatorAddress localAddress, boolean useRemoteLogger) {
        if (useRemoteLogger) {
            return new RemoteExceptionLogger(localAddress, WORKER_EXCEPTION, this);
        } else {
            return new FileExceptionLogger(localAddress, WORKER_EXCEPTION);
        }
    }
}
