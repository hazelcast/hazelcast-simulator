/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.simulator.protocol.processors;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.simulator.common.TestCase;
import com.hazelcast.simulator.common.WorkerType;
import com.hazelcast.simulator.protocol.connector.WorkerConnector;
import com.hazelcast.simulator.protocol.core.Response;
import com.hazelcast.simulator.protocol.core.ResponseFuture;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.exception.ProcessException;
import com.hazelcast.simulator.protocol.operation.CreateTestOperation;
import com.hazelcast.simulator.protocol.operation.ExecuteScriptOperation;
import com.hazelcast.simulator.protocol.operation.IntegrationTestOperation;
import com.hazelcast.simulator.protocol.operation.LogOperation;
import com.hazelcast.simulator.protocol.operation.OperationType;
import com.hazelcast.simulator.protocol.operation.SimulatorOperation;
import com.hazelcast.simulator.protocol.operation.TerminateWorkerOperation;
import com.hazelcast.simulator.utils.ExceptionReporter;
import com.hazelcast.simulator.worker.Promise;
import com.hazelcast.simulator.worker.ScriptExecutor;
import com.hazelcast.simulator.worker.Worker;
import com.hazelcast.simulator.worker.testcontainer.TestContainer;
import com.hazelcast.simulator.worker.testcontainer.TestContextImpl;
import org.apache.log4j.Logger;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.simulator.protocol.core.ResponseType.SUCCESS;
import static com.hazelcast.simulator.protocol.core.ResponseType.UNSUPPORTED_OPERATION_ON_THIS_PROCESSOR;
import static com.hazelcast.simulator.protocol.operation.IntegrationTestOperation.Type.DEEP_NESTED_ASYNC;
import static com.hazelcast.simulator.protocol.operation.IntegrationTestOperation.Type.DEEP_NESTED_SYNC;
import static com.hazelcast.simulator.utils.CommonUtils.sleepSeconds;
import static com.hazelcast.simulator.utils.TestUtils.getUserContextKeyFromTestId;
import static java.lang.String.format;

/**
 * An {@link OperationProcessor} implementation to process {@link SimulatorOperation} instances on a Simulator Worker.
 */
public class WorkerOperationProcessor extends AbstractOperationProcessor {

    private static final String DASHES = "---------------------------";

    private static final Logger LOGGER = Logger.getLogger(WorkerOperationProcessor.class);

    private final ConcurrentMap<String, TestContainer> tests = new ConcurrentHashMap<String, TestContainer>();

    private final WorkerType type;
    private final HazelcastInstance hazelcastInstance;
    private final Worker worker;
    private final SimulatorAddress workerAddress;
    private final ScriptExecutor scriptExecutor;

    public WorkerOperationProcessor(WorkerType type, HazelcastInstance hazelcastInstance,
                                    Worker worker, SimulatorAddress workerAddress) {
        this.type = type;
        this.hazelcastInstance = hazelcastInstance;
        this.worker = worker;
        this.workerAddress = workerAddress;
        this.scriptExecutor = new ScriptExecutor(hazelcastInstance);
    }

    public Collection<TestContainer> getTests() {
        return tests.values();
    }

    @Override
    protected void processOperation(OperationType operationType, SimulatorOperation op,
                                    SimulatorAddress sourceAddress, Promise promise) throws Exception {
        switch (operationType) {
            case INTEGRATION_TEST:
                processIntegrationTest((IntegrationTestOperation) op, sourceAddress, promise);
                return;
            case PING:
                processPing(sourceAddress);
                promise.answer(SUCCESS);
                break;
            case TERMINATE_WORKER:
                processTerminateWorker((TerminateWorkerOperation) op);
                promise.answer(SUCCESS);
                break;
            case CREATE_TEST:
                processCreateTest((CreateTestOperation) op);
                promise.answer(SUCCESS);
                break;
            case EXECUTE_SCRIPT:
                scriptExecutor.execute((ExecuteScriptOperation) op, promise);
                break;
            default:
                throw new ProcessException(UNSUPPORTED_OPERATION_ON_THIS_PROCESSOR);
        }
    }

    @Override
    protected void onProcessOperationFailure(Throwable t) {
        ExceptionReporter.report(null, t);
    }

    private void processIntegrationTest(IntegrationTestOperation operation, SimulatorAddress sourceAddress, Promise promise)
            throws Exception {
        SimulatorOperation nestedOperation;
        Response response;
        ResponseFuture future;
        switch (operation.getType()) {
            case NESTED_SYNC:
                nestedOperation = new LogOperation("Sync nested integration test message");
                response = worker.getWorkerConnector().invoke(sourceAddress, nestedOperation);
                LOGGER.debug("Got response for sync nested message: " + response);
                break;
            case NESTED_ASYNC:
                nestedOperation = new LogOperation("Async nested integration test message");
                future = worker.getWorkerConnector().submit(sourceAddress, nestedOperation);
                response = future.get();
                LOGGER.debug("Got response for async nested message: " + response);
                break;
            case DEEP_NESTED_SYNC:
                nestedOperation = new IntegrationTestOperation(DEEP_NESTED_SYNC);
                response = worker.getWorkerConnector().invoke(workerAddress.getParent(), nestedOperation);
                LOGGER.debug("Got response for sync deep nested message: " + response);
                break;
            case DEEP_NESTED_ASYNC:
                nestedOperation = new IntegrationTestOperation(DEEP_NESTED_ASYNC);
                future = worker.getWorkerConnector().submit(workerAddress.getParent(), nestedOperation);
                response = future.get();
                LOGGER.debug("Got response for async deep nested message: " + response);
                break;
            default:
                throw new ProcessException(UNSUPPORTED_OPERATION_ON_THIS_PROCESSOR);
        }

        promise.answer(response.getFirstErrorResponseType());
    }

    private void processPing(SimulatorAddress sourceAddress) {
        WorkerConnector workerConnector = worker.getWorkerConnector();
        LOGGER.debug(format("Pinged by %s (queue size: %d)...", sourceAddress, workerConnector.getMessageQueueSize()));
    }

    private void processTerminateWorker(TerminateWorkerOperation operation) {
        LOGGER.warn("Terminating worker");
        if (type == WorkerType.MEMBER) {
            sleepSeconds(operation.getMemberWorkerShutdownDelaySeconds());
        }
        worker.shutdown(operation.isEnsureProcessShutdown());
    }

    private void processCreateTest(CreateTestOperation operation) {
        LOGGER.info(operation.getTestCase().toString());

        TestCase testCase = operation.getTestCase();
        int testIndex = operation.getTestIndex();
        WorkerConnector workerConnector = worker.getWorkerConnector();
        if (workerConnector.getTest(testIndex) != null) {
            throw new IllegalStateException(format("Can't init TestCase: %s, another test with testIndex %d already exists",
                    operation, testIndex));
        }
        String testId = testCase.getId();
        if (tests.containsKey(testId)) {
            throw new IllegalStateException(format("Can't init TestCase: %s, another test with testId [%s] already exists",
                    operation, testId));
        }

        LOGGER.info(format("%s Initializing test %s %s%n%s", DASHES, testId, DASHES, testCase));

        TestContextImpl testContext = new TestContextImpl(
                hazelcastInstance, testId, worker.getPublicIpAddress(), workerConnector);

        TestContainer testContainer = new TestContainer(testContext, testCase);
        SimulatorAddress testAddress = workerAddress.getChild(testIndex);
        TestOperationProcessor processor = new TestOperationProcessor(worker, type, testContainer, testAddress);

        workerConnector.addTest(testIndex, processor);
        tests.put(testId, testContainer);

        if (type == WorkerType.MEMBER) {
            hazelcastInstance.getUserContext().put(getUserContextKeyFromTestId(testId), testContainer.getTestInstance());
        }
    }

    public void remove(String id) {
        tests.remove(id);
    }
}
