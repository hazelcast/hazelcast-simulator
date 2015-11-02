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
import com.hazelcast.simulator.protocol.configuration.WorkerServerConfiguration;
import com.hazelcast.simulator.protocol.core.ConnectionManager;
import com.hazelcast.simulator.protocol.core.ResponseFuture;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.exception.ExceptionLogger;
import com.hazelcast.simulator.protocol.exception.ExceptionType;
import com.hazelcast.simulator.protocol.exception.FileExceptionLogger;
import com.hazelcast.simulator.protocol.exception.RemoteExceptionLogger;
import com.hazelcast.simulator.protocol.operation.SimulatorOperation;
import com.hazelcast.simulator.protocol.processors.TestOperationProcessor;
import com.hazelcast.simulator.protocol.processors.WorkerOperationProcessor;
import com.hazelcast.simulator.worker.Worker;
import com.hazelcast.simulator.worker.WorkerType;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.simulator.protocol.core.AddressLevel.WORKER;

/**
 * Connector which listens for incoming Simulator Agent connections and manages Simulator Test instances.
 */
public class WorkerConnector extends AbstractServerConnector {

    private final WorkerServerConfiguration workerServerConfiguration;

    WorkerConnector(WorkerServerConfiguration configuration) {
        super(configuration);
        this.workerServerConfiguration = configuration;
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
        SimulatorAddress localAddress = new SimulatorAddress(WORKER, parentAddressIndex, addressIndex, 0);

        ExceptionLogger exceptionLogger;
        if (useRemoteLogger) {
            exceptionLogger = new RemoteExceptionLogger(localAddress, ExceptionType.WORKER_EXCEPTION);
        } else {
            exceptionLogger = new FileExceptionLogger(localAddress, ExceptionType.WORKER_EXCEPTION);
        }
        WorkerOperationProcessor processor = new WorkerOperationProcessor(exceptionLogger, type, hazelcastInstance, worker,
                localAddress);
        ConcurrentMap<String, ResponseFuture> futureMap = new ConcurrentHashMap<String, ResponseFuture>();
        ConnectionManager connectionManager = new ConnectionManager();

        WorkerServerConfiguration configuration = new WorkerServerConfiguration(processor, futureMap, connectionManager,
                localAddress, port);

        WorkerConnector connector = new WorkerConnector(configuration);

        if (useRemoteLogger) {
            ((RemoteExceptionLogger) exceptionLogger).setServerConnector(connector);
        }
        return connector;
    }

    /**
     * Gets a Simulator Test.
     *
     * @param testIndex the index of the Simulator Test
     * @return the {@link TestOperationProcessor} which processes incoming
     * {@link com.hazelcast.simulator.protocol.operation.SimulatorOperation} for this test
     */
    public TestOperationProcessor getTest(int testIndex) {
        return workerServerConfiguration.getTest(testIndex);
    }

    /**
     * Adds a Simulator Test.
     *
     * @param testIndex the index of the Simulator Test
     * @param processor the {@link TestOperationProcessor} which processes incoming
     *                  {@link com.hazelcast.simulator.protocol.operation.SimulatorOperation} for this test
     */
    public void addTest(int testIndex, TestOperationProcessor processor) {
        workerServerConfiguration.addTest(testIndex, processor);
    }

    /**
     * Removes a Simulator Test.
     *
     * @param testIndex the index of the remote Simulator Test
     */
    public void removeTest(int testIndex) {
        workerServerConfiguration.removeTest(testIndex);
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
}
