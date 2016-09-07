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
package com.hazelcast.simulator.protocol.processors;

import com.hazelcast.simulator.protocol.connector.WorkerConnector;
import com.hazelcast.simulator.protocol.core.Response;
import com.hazelcast.simulator.protocol.core.ResponseFuture;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.core.SimulatorMessage;
import com.hazelcast.simulator.protocol.exception.ProcessException;
import com.hazelcast.simulator.protocol.operation.CreateTestOperation;
import com.hazelcast.simulator.protocol.operation.ExecuteScriptOperation;
import com.hazelcast.simulator.protocol.operation.IntegrationTestOperation;
import com.hazelcast.simulator.protocol.operation.LogOperation;
import com.hazelcast.simulator.protocol.operation.SimulatorOperation;
import com.hazelcast.simulator.protocol.operation.StartTestOperation;
import com.hazelcast.simulator.protocol.operation.StartTestPhaseOperation;
import com.hazelcast.simulator.protocol.operation.TerminateWorkerOperation;
import com.hazelcast.simulator.utils.ExceptionReporter;
import com.hazelcast.simulator.worker.Promise;
import com.hazelcast.simulator.worker.ScriptExecutor;
import com.hazelcast.simulator.worker.Worker;
import com.hazelcast.simulator.worker.testcontainer.TestContainerManager;
import org.apache.log4j.Logger;

import static com.hazelcast.simulator.protocol.core.ResponseType.SUCCESS;
import static com.hazelcast.simulator.protocol.core.ResponseType.UNSUPPORTED_OPERATION_ON_THIS_PROCESSOR;
import static com.hazelcast.simulator.protocol.operation.IntegrationTestOperation.Type.DEEP_NESTED_ASYNC;
import static com.hazelcast.simulator.protocol.operation.IntegrationTestOperation.Type.DEEP_NESTED_SYNC;
import static java.lang.String.format;

/**
 * An {@link OperationProcessor} implementation to process {@link SimulatorOperation} instances on a Simulator Worker.
 */
public class WorkerOperationProcessor extends AbstractOperationProcessor {

    private static final Logger LOGGER = Logger.getLogger(WorkerOperationProcessor.class);

    private final Worker worker;
    private final TestContainerManager testContainerManager;
    private final SimulatorAddress workerAddress;
    private final ScriptExecutor scriptExecutor;

    public WorkerOperationProcessor(TestContainerManager testContainerManager,
                                    ScriptExecutor scriptExecutor,
                                    Worker worker,
                                    SimulatorAddress workerAddress) {
        this.testContainerManager = testContainerManager;
        this.worker = worker;
        this.scriptExecutor = scriptExecutor;
        this.workerAddress = workerAddress;
    }

    public TestContainerManager getTestContainerManager() {
        return testContainerManager;
    }

    @Override
    protected void processOperation(SimulatorMessage msg, SimulatorOperation op, Promise promise) throws Exception {
        switch (msg.getOperationType()) {
            case INTEGRATION_TEST:
                processIntegrationTest((IntegrationTestOperation) op, msg.getSource(), promise);
                return;
            case PING:
                WorkerConnector workerConnector = worker.getWorkerConnector();
                LOGGER.debug(format("Pinged by %s (queue size: %d)...", msg.getSource(), workerConnector.getMessageQueueSize()));
                promise.answer(SUCCESS);
                break;
            case TERMINATE_WORKER:
                processTerminateWorker((TerminateWorkerOperation) op);
                promise.answer(SUCCESS);
                break;
            case CREATE_TEST:
                CreateTestOperation createTestOperation = (CreateTestOperation) op;
                testContainerManager.createTest(createTestOperation);
                promise.answer(SUCCESS);
                break;
            case EXECUTE_SCRIPT:
                scriptExecutor.execute((ExecuteScriptOperation) op, promise);
                return;
            case START_TEST_PHASE:
                StartTestPhaseOperation startTestPhaseOperation = (StartTestPhaseOperation) op;
                testContainerManager.startTestPhase(msg.getDestination(), startTestPhaseOperation.getTestPhase());
                promise.answer(SUCCESS);
                break;
            case START_TEST:
                testContainerManager.start((StartTestOperation) op, msg.getDestination());
                promise.answer(SUCCESS);
                break;
            case STOP_TEST:
                testContainerManager.stop(msg.getDestination());
                promise.answer(SUCCESS);
                break;
            default:
                throw new ProcessException("Unsupported operation:" + op, UNSUPPORTED_OPERATION_ON_THIS_PROCESSOR);
        }
    }

    @Override
    protected void onProcessOperationFailure(Throwable t) {
        ExceptionReporter.report(null, t);
    }

    private void processIntegrationTest(IntegrationTestOperation op, SimulatorAddress sourceAddress, Promise promise)
            throws Exception {
        SimulatorOperation nestedOperation;
        Response response;
        ResponseFuture future;
        switch (op.getType()) {
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

    private void processTerminateWorker(TerminateWorkerOperation op) {
        LOGGER.warn("Terminating worker");
//        if (type == WorkerType.MEMBER) {
//            sleepSeconds(operation.getMemberWorkerShutdownDelaySeconds());
//        }
        worker.shutdown(op.isEnsureProcessShutdown());
    }
}
