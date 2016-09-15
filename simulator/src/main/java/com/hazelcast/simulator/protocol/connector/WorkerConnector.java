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
package com.hazelcast.simulator.protocol.connector;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.simulator.common.WorkerType;
import com.hazelcast.simulator.protocol.core.ConnectionManager;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.handler.ConnectionListenerHandler;
import com.hazelcast.simulator.protocol.handler.ConnectionValidationHandler;
import com.hazelcast.simulator.protocol.handler.ExceptionHandler;
import com.hazelcast.simulator.protocol.handler.MessageConsumeHandler;
import com.hazelcast.simulator.protocol.handler.MessageEncoder;
import com.hazelcast.simulator.protocol.handler.ResponseEncoder;
import com.hazelcast.simulator.protocol.handler.ResponseHandler;
import com.hazelcast.simulator.protocol.handler.SimulatorFrameDecoder;
import com.hazelcast.simulator.protocol.handler.SimulatorProtocolDecoder;
import com.hazelcast.simulator.protocol.operation.SimulatorOperation;
import com.hazelcast.simulator.protocol.processors.OperationProcessor;
import com.hazelcast.simulator.protocol.processors.WorkerOperationProcessor;
import com.hazelcast.simulator.worker.ScriptExecutor;
import com.hazelcast.simulator.worker.Worker;
import com.hazelcast.simulator.worker.testcontainer.TestContainerManager;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.group.ChannelGroup;

import static com.hazelcast.simulator.protocol.core.AddressLevel.WORKER;

/**
 * Connector which listens for incoming Simulator Agent connections and manages Simulator Test instances.
 */
public class WorkerConnector extends AbstractServerConnector {

    private static final int DEFAULT_THREAD_POOL_SIZE = 3;

    private final OperationProcessor processor;

    private final int addressIndex;

    private final ConnectionManager connectionManager = new ConnectionManager();

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
    public WorkerConnector(int parentAddressIndex,
                           int addressIndex,
                           int port,
                           WorkerType type,
                           HazelcastInstance hazelcastInstance,
                           Worker worker) {
        super(new SimulatorAddress(WORKER, parentAddressIndex, addressIndex, 0), port, DEFAULT_THREAD_POOL_SIZE);

        TestContainerManager testContainerManager = new TestContainerManager(
                hazelcastInstance, getAddress(), worker.getPublicIpAddress(), this, type);
        ScriptExecutor scriptExecutor = new ScriptExecutor(hazelcastInstance);
        this.processor = new WorkerOperationProcessor(testContainerManager, scriptExecutor, worker, getAddress());
        this.addressIndex = localAddress.getAddressIndex();
    }

    @Override
    void configureServerPipeline(ChannelPipeline pipeline, ServerConnector serverConnector) {
        pipeline.addLast("connectionValidationHandler", new ConnectionValidationHandler());
        pipeline.addLast("connectionListenerHandler", new ConnectionListenerHandler(connectionManager));
        pipeline.addLast("responseEncoder", new ResponseEncoder(localAddress));
        pipeline.addLast("messageEncoder", new MessageEncoder(localAddress, localAddress.getParent()));
        pipeline.addLast("frameDecoder", new SimulatorFrameDecoder());
        pipeline.addLast("protocolDecoder", new SimulatorProtocolDecoder(localAddress));
        pipeline.addLast("messageConsumeHandler", new MessageConsumeHandler(localAddress, processor, getScheduledExecutor()));
        pipeline.addLast("testProtocolDecoder", new SimulatorProtocolDecoder(localAddress.getChild(0)));
        pipeline.addLast("responseHandler", new ResponseHandler(localAddress, localAddress.getParent(), futureMap, addressIndex));
        pipeline.addLast("exceptionHandler", new ExceptionHandler(serverConnector));
    }

    @Override
    ChannelGroup getChannelGroup() {
        return connectionManager.getChannels();
    }

    /**
     * Returns the size of the internal message queue used by {@link #submit(SimulatorAddress, SimulatorOperation)}.
     *
     * @return the message queue size
     */
    public int getMessageQueueSize() {
        return getMessageQueueSizeInternal();
    }

    public OperationProcessor getProcessor() {
        return processor;
    }
}
