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

import com.hazelcast.simulator.agent.Agent;
import com.hazelcast.simulator.agent.workerprocess.WorkerProcessLauncher;
import com.hazelcast.simulator.agent.workerprocess.WorkerProcessManager;
import com.hazelcast.simulator.agent.workerprocess.WorkerProcessSettings;
import com.hazelcast.simulator.common.WorkerType;
import com.hazelcast.simulator.protocol.core.Response;
import com.hazelcast.simulator.protocol.core.ResponseFuture;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.exception.ProcessException;
import com.hazelcast.simulator.protocol.operation.CreateWorkerOperation;
import com.hazelcast.simulator.protocol.operation.InitSessionOperation;
import com.hazelcast.simulator.protocol.operation.IntegrationTestOperation;
import com.hazelcast.simulator.protocol.operation.LogOperation;
import com.hazelcast.simulator.protocol.operation.OperationType;
import com.hazelcast.simulator.protocol.operation.SimulatorOperation;
import com.hazelcast.simulator.worker.Promise;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;

import static com.hazelcast.simulator.protocol.core.ResponseType.EXCEPTION_DURING_OPERATION_EXECUTION;
import static com.hazelcast.simulator.protocol.core.ResponseType.SUCCESS;
import static com.hazelcast.simulator.protocol.core.ResponseType.UNSUPPORTED_OPERATION_ON_THIS_PROCESSOR;
import static com.hazelcast.simulator.protocol.core.SimulatorAddress.COORDINATOR;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.log4j.Level.DEBUG;
import static org.apache.log4j.Level.FATAL;

/**
 * An {@link OperationProcessor} implementation to process {@link SimulatorOperation} instances on a Simulator Agent.
 */
public class AgentOperationProcessor extends AbstractOperationProcessor {

    private static final Logger LOGGER = Logger.getLogger(AgentOperationProcessor.class);

    private final Agent agent;
    private final WorkerProcessManager workerProcessManager;
    private final ScheduledExecutorService executorService;

    public AgentOperationProcessor(Agent agent,
                                   WorkerProcessManager workerProcessManager,
                                   ScheduledExecutorService executorService) {
        this.agent = agent;
        this.workerProcessManager = workerProcessManager;
        this.executorService = executorService;
    }

    @Override
    protected void processOperation(OperationType operationType, SimulatorOperation op,
                                    SimulatorAddress sourceAddress, Promise promise) throws Exception {
        switch (operationType) {
            case INTEGRATION_TEST:
                processIntegrationTest((IntegrationTestOperation) op, sourceAddress, promise);
                return;
            case INIT_SESSION:
                agent.setSessionId(((InitSessionOperation) op).getSessionId());
                promise.answer(SUCCESS);
                return;
            case CREATE_WORKER:
                processCreateWorker((CreateWorkerOperation) op, promise);
                return;
            case START_TIMEOUT_DETECTION:
                processStartTimeoutDetection();
                promise.answer(SUCCESS);
                return;
            case STOP_TIMEOUT_DETECTION:
                processStopTimeoutDetection();
                promise.answer(SUCCESS);
                return;
            default:
                throw new ProcessException(UNSUPPORTED_OPERATION_ON_THIS_PROCESSOR);
        }
    }

    private void processIntegrationTest(
            IntegrationTestOperation op, SimulatorAddress sourceAddress, Promise promise) throws Exception {
        SimulatorOperation nestedOperation;
        Response response;
        ResponseFuture future;
        switch (op.getType()) {
            case NESTED_SYNC:
                nestedOperation = new LogOperation("Sync nested integration test message");
                response = agent.getAgentConnector().invoke(sourceAddress, nestedOperation);
                LOGGER.debug("Got response for sync nested message: " + response);
                promise.answer(response.getFirstErrorResponseType());
                return;
            case NESTED_ASYNC:
                nestedOperation = new LogOperation("Async nested integration test message");
                future = agent.getAgentConnector().submit(sourceAddress, nestedOperation);
                response = future.get();
                LOGGER.debug("Got response for async nested message: " + response);
                promise.answer(response.getFirstErrorResponseType());
                return;
            case DEEP_NESTED_SYNC:
                nestedOperation = new LogOperation("Sync deep nested integration test message");
                response = agent.getAgentConnector().invoke(COORDINATOR, nestedOperation);
                LOGGER.debug("Got response for sync deep nested message: " + response);
                promise.answer(response.getFirstErrorResponseType());
                return;
            case DEEP_NESTED_ASYNC:
                nestedOperation = new LogOperation("Sync deep nested integration test message");
                future = agent.getAgentConnector().submit(COORDINATOR, nestedOperation);
                response = future.get();
                LOGGER.debug("Got response for async deep nested message: " + response);
                promise.answer(response.getFirstErrorResponseType());
                return;
            default:
                throw new ProcessException(UNSUPPORTED_OPERATION_ON_THIS_PROCESSOR);
        }
    }

    private void processCreateWorker(CreateWorkerOperation op, Promise promise) throws Exception {
        ArrayList<Future<Boolean>> futures = new ArrayList<Future<Boolean>>();
        for (WorkerProcessSettings workerProcessSettings : op.getWorkerProcessSettings()) {
            WorkerProcessLauncher launcher = new WorkerProcessLauncher(agent, workerProcessManager, workerProcessSettings);
            LaunchWorkerCallable task = new LaunchWorkerCallable(launcher, workerProcessSettings);
            Future<Boolean> future = executorService.schedule(task, op.getDelayMs(), MILLISECONDS);
            futures.add(future);
        }
        for (Future<Boolean> future : futures) {
            if (!future.get()) {
                LOGGER.error("Failed to start Worker, settings response type EXCEPTION_DURING_OPERATION_EXECUTION...");
                throw new ProcessException(EXCEPTION_DURING_OPERATION_EXECUTION);
            }
        }
        promise.answer(SUCCESS);
    }

    private void processStartTimeoutDetection() {
        agent.getWorkerProcessFailureMonitor().startTimeoutDetection();
    }

    private void processStopTimeoutDetection() {
        agent.getWorkerProcessFailureMonitor().stopTimeoutDetection();
    }

    private final class LaunchWorkerCallable implements Callable<Boolean> {

        private final WorkerProcessLauncher launcher;
        private final WorkerProcessSettings workerProcessSettings;

        private LaunchWorkerCallable(WorkerProcessLauncher launcher, WorkerProcessSettings workerProcessSettings) {
            this.launcher = launcher;
            this.workerProcessSettings = workerProcessSettings;
        }

        @Override
        public Boolean call() {
            try {
                launcher.launch();

                int workerIndex = workerProcessSettings.getWorkerIndex();
                int workerPort = agent.getPort() + workerIndex;
                SimulatorAddress workerAddress = agent.getAgentConnector().addWorker(workerIndex, "127.0.0.1", workerPort);

                WorkerType workerType = workerProcessSettings.getWorkerType();

                LogOperation logOperation = new LogOperation(format("Created %s Worker %s", workerType, workerAddress), DEBUG);
                agent.getAgentConnector().submit(COORDINATOR, logOperation);

                return true;
            } catch (Exception e) {
                LOGGER.error("Failed to start Worker", e);

                LogOperation logOperation = new LogOperation("Failed to start Worker: " + e.getMessage(), FATAL);
                agent.getAgentConnector().submit(COORDINATOR, logOperation);

                return false;
            }
        }
    }
}
